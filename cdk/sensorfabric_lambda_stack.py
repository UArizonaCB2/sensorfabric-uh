from typing import Dict, Any
from dataclasses import dataclass
import aws_cdk as cdk
from aws_cdk import (
    aws_lambda as lambda_,
    aws_ecr as ecr,
    aws_iam as iam,
    aws_logs as logs,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_sqs as sqs,
    aws_events as events,
    aws_events_targets as targets,
    Duration,
    Stack,
    RemovalPolicy
)
from constructs import Construct


@dataclass
class StackConfig:
    """Configuration for SensorFabric Lambda Stack deployment."""
    stack_name: str           # e.g., "UltraHuman-AZ-1"
    environment: str          # dev, staging, prod
    ecr_registry: str
    ecr_repository: str
    project_name: str
    database_name: str
    sns_topic_name: str
    aws_secret_name: str
    sf_data_bucket: str
    uh_environment: str


class SensorFabricLambdaStack(Stack):
    """
    CDK Stack for SensorFabric Lambda functions using Docker containers.
    
    This stack creates:
    - Lambda functions from ECR container images
    - IAM roles and policies
    - CloudWatch log groups
    - SNS topics for inter-function communication
    - EventBridge rules for scheduling
    """

    def __init__(self, scope: Construct, construct_id: str, config: StackConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Store configuration
        self.config = config
        self.lambda_functions = {}
        self.lambda_aliases = {}

        # Validate configuration
        self._validate_config(config)

    def _validate_config(self, config: StackConfig) -> None:
        """Validate the stack configuration."""
        required_fields = [
            'stack_name', 'environment', 'ecr_registry', 'ecr_repository',
            'project_name', 'database_name', 'sns_topic_name', 'aws_secret_name',
            'sf_data_bucket', 'uh_environment'
        ]
        
        for field in required_fields:
            value = getattr(config, field)
            if not value or not isinstance(value, str) or not value.strip():
                raise ValueError(f"Configuration field '{field}' is required and must be a non-empty string")
        
        # Validate stack_name format (AWS resource naming conventions)
        if not config.stack_name.replace('-', '').replace('_', '').isalnum():
            raise ValueError(f"Stack name '{config.stack_name}' must contain only alphanumeric characters, hyphens, and underscores")
        
        # Validate environment
        valid_environments = ['dev', 'staging', 'prod', 'production']
        if config.environment not in valid_environments:
            raise ValueError(f"Environment '{config.environment}' must be one of: {valid_environments}")

        # Environment variables:
        # biobayb_uh_sns_publisher: AWS_SECRET_NAME, UH_DLQ_URL, UH_SNS_TOPIC_ARN
        # biobayb_uh_uploader: SF_DATA_BUCKET, UH_ENVIRONMENT, AWS_SECRET_NAME
        # Both functions have access to AWS Secrets Manager secret 'prod/biobayb/uh/keys'

        # lambda configs (environment variables will be updated after resource creation)
        self.lambda_config = {
            "biobayb_uh_uploader": {
                "description": "UltraHuman data uploader Lambda function",
                "timeout": Duration.minutes(15),
                "memory_size": 3008,
                "environment": {
                    "UH_ENVIRONMENT": self.config.uh_environment,
                    "SF_DATA_BUCKET": self.config.sf_data_bucket,
                    "AWS_SECRET_NAME": self.config.aws_secret_name
                }
            },
            "biobayb_uh_sns_publisher": {
                "description": "UltraHuman SNS publisher Lambda function",
                "timeout": Duration.minutes(5),
                "memory_size": 2048,
                "environment": {
                    "AWS_SECRET_NAME": self.config.aws_secret_name,
                    "UH_ENVIRONMENT": self.config.uh_environment
                }
            }
        }

        # Create ECR repository reference
        self.ecr_repo = ecr.Repository.from_repository_name(
            self, "SensorFabricECRRepo", 
            repository_name=self.config.ecr_repository
        )

        # Create IAM roles
        self.create_iam_roles()
        
        # Create SNS topics and subscriptions
        self.create_sns_resources()
        
        # Create SQS resources
        self.create_sqs_resources()
        
        # Create Lambda functions (after SNS/SQS to reference ARNs)
        self.create_lambda_functions()
        
        # Create Lambda aliases with provisioned concurrency
        self.create_lambda_aliases()

        self.subscribe_sns_to_lambda()

        # Create EventBridge rules for scheduling
        self.create_eventbridge_rules()

    def create_iam_roles(self) -> None:
        """Create IAM roles for Lambda functions."""
        
        # Base Lambda execution role
        self.lambda_execution_role = iam.Role(
            self, f"{self.config.project_name}_LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaVPCAccessExecutionRole")
            ]
        )

        # Additional policies for SensorFabric operations
        sensorfabric_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                # S3 permissions for data storage
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                
                # Athena permissions for query execution
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:StopQueryExecution",
                "athena:GetWorkGroup",
                
                # Glue permissions for data catalog
                "glue:GetDatabase",
                "glue:CreateDatabase",
                "glue:GetTable",
                "glue:GetPartitions",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:CreatePartition",
                "glue:BatchCreatePartition",
                "glue:GetDatabases",
                "glue:GetTables",
                
                # SNS permissions for messaging
                "sns:Publish",
                "sns:Subscribe",
                "sns:Unsubscribe",
                "sns:ListTopics",
                "sns:GetTopicAttributes",
                
                # CloudWatch permissions for monitoring
                "cloudwatch:PutMetricData",
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                
                # SQS permissions for dead letter queue
                "sqs:SendMessage",
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes"
            ],
            resources=["*"]
        )
        
        # Secrets Manager policy for accessing UltraHuman keys
        secrets_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret"
            ],
            resources=[
                f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:{self.config.aws_secret_name}*"
            ]
        )

        self.lambda_execution_role.add_to_policy(sensorfabric_policy)
        self.lambda_execution_role.add_to_policy(secrets_policy)

    def create_lambda_functions(self) -> None:
        """Create Lambda functions from ECR container images."""

        for function_name, config in self.lambda_config.items():
            # Create CloudWatch Log Group
            log_group = logs.LogGroup(
                self, f"{self.config.project_name}_{function_name}_LogGroup",
                log_group_name=f"/aws/lambda/{function_name}",
                retention=logs.RetentionDays.ONE_MONTH,
                removal_policy=RemovalPolicy.DESTROY
            )

            # Create Lambda function
            lambda_function = lambda_.DockerImageFunction(
                self, f"{self.config.project_name}_{function_name}_Lambda",
                function_name=f"{self.config.project_name}_{function_name}_Lambda",
                description=config["description"],
                code=lambda_.DockerImageCode.from_ecr(
                    repository=self.ecr_repo,
                    tag_or_digest=function_name
                ),
                role=self.lambda_execution_role,
                timeout=config["timeout"],
                memory_size=config["memory_size"],
                environment=config["environment"],
                log_group=log_group,
                
                # Container-specific settings
                architecture=lambda_.Architecture.X86_64,
                
                # Dead letter queue for failed executions
                dead_letter_queue_enabled=True,
                
                # Retry configuration
                retry_attempts=2
            )

            self.lambda_functions[function_name] = lambda_function

            # Output the Lambda function ARN
            cdk.CfnOutput(
                self, f"{self.config.project_name}_{function_name}_Lambda_ARN",
                value=lambda_function.function_arn,
                description=f"ARN for {self.config.project_name}_{function_name} Lambda function"
            )
        
        # Add dynamic environment variables after all resources are created
        self.update_lambda_environment_variables()

    def create_lambda_aliases(self) -> None:
        """Create Lambda aliases with provisioned concurrency."""
        
        for function_name, lambda_function in self.lambda_functions.items():
            # Create alias pointing to $LATEST
            alias = lambda_.Alias(
                self, f"{self.config.project_name}_{function_name}_Alias",
                alias_name="LIVE",
                version=lambda_function.current_version,
                provisioned_concurrent_executions=1,
                description=f"LIVE alias for {function_name} with provisioned concurrency"
            )
            
            self.lambda_aliases[function_name] = alias
            
            # Output the alias ARN
            cdk.CfnOutput(
                self, f"{self.config.project_name}_{function_name}_Alias_ARN",
                value=alias.function_arn,
                description=f"ARN for {self.config.project_name}_{function_name} LIVE alias"
            )

    def create_sns_resources(self) -> None:
        """Create SNS topics for inter-function communication."""
        
        # Topic for UltraHuman data collection requests
        self.uh_data_collection_topic = sns.Topic(
            self, "UHDataCollectionTopic",
            topic_name=f"{self.config.stack_name}-{self.config.sns_topic_name}",
            display_name="UltraHuman Data Collection Topic",
        )

        # Output SNS topic ARN
        cdk.CfnOutput(
            self, "UHDataCollectionTopicARN",
            value=self.uh_data_collection_topic.topic_arn,
            description="ARN for UltraHuman data collection SNS topic"
        )

    def subscribe_sns_to_lambda(self) -> None:
        """
        Subscribe the uploader Lambda alias to the topic and grant publish permissions to the publisher Lambda alias.
        To be run after lambdas and aliases are created.
        """
        # Subscribe the uploader Lambda alias to the topic
        if "biobayb_uh_uploader" in self.lambda_aliases:
            self.uh_data_collection_topic.add_subscription(
                subscriptions.LambdaSubscription(self.lambda_aliases["biobayb_uh_uploader"])
            )

        # Grant publish permissions to the publisher Lambda alias
        if "biobayb_uh_sns_publisher" in self.lambda_aliases:
            self.uh_data_collection_topic.grant_publish(
                self.lambda_aliases["biobayb_uh_sns_publisher"]
            )

    def create_sqs_resources(self) -> None:
        """Create SQS resources for dead letter queues."""
        
        # Dead letter queue for SNS publisher
        self.uh_dlq = sqs.Queue(
            self, "UHPublisherDLQ",
            queue_name=f"{self.config.stack_name}-biobayb_uh_undeliverable",
            visibility_timeout=Duration.seconds(300),
            retention_period=Duration.days(14),
            removal_policy=RemovalPolicy.DESTROY
        )
        
        # Output DLQ ARN
        cdk.CfnOutput(
            self, "UHPublisherDLQARN",
            value=self.uh_dlq.queue_arn,
            description="ARN for UltraHuman publisher dead letter queue"
        )

    def create_eventbridge_rules(self) -> None:
        """Create EventBridge rules for scheduled Lambda executions using aliases."""
        
        # Schedule for SNS publisher (runs daily at midnight AZ time UTC-7)
        if "biobayb_uh_sns_publisher" in self.lambda_aliases:
            publisher_rule = events.Rule(
                self, f"{self.config.project_name}_UHPublisherScheduleRule",
                description="Schedule for UltraHuman SNS publisher",
                schedule=events.Schedule.cron(
                    minute="0",
                    hour="7",
                    day="*",
                    month="*",
                    year="*"
                )
            )
            
            publisher_rule.add_target(
                targets.LambdaFunction(self.lambda_aliases["biobayb_uh_sns_publisher"])
            )

        # Manual trigger capability for uploader
        if "biobayb_uh_uploader" in self.lambda_aliases:
            # This creates a custom event pattern that can be triggered manually
            uploader_rule = events.Rule(
                self, f"{self.config.project_name}_UHUploaderManualTriggerRule",
                description="Manual trigger for UltraHuman data uploader",
                event_pattern=events.EventPattern(
                    source=["sensorfabric.manual"],
                    detail_type=["UltraHuman Data Upload Request"]
                )
            )
            
            uploader_rule.add_target(
                targets.LambdaFunction(self.lambda_aliases["biobayb_uh_uploader"])
            )

    def update_lambda_environment_variables(self) -> None:
        """Update Lambda functions with dynamic environment variables."""
        
        # Add SNS topic ARN and DLQ URL to SNS publisher
        if "biobayb_uh_sns_publisher" in self.lambda_functions:
            publisher_lambda = self.lambda_functions["biobayb_uh_sns_publisher"]
            publisher_lambda.add_environment("UH_SNS_TOPIC_ARN", self.uh_data_collection_topic.topic_arn)
            publisher_lambda.add_environment("UH_DLQ_URL", self.uh_dlq.queue_url)
