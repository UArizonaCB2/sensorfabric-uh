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
    aws_stepfunctions as stepfunctions,
    aws_stepfunctions_tasks as stepfunctions_tasks,
    Tags,
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
    template_mode: str        # PRODUCTION or PRESENT
    jwt_expiration_days: str


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
        
        # add tags
        Tags.of(self).add("project", self.config.project_name)


    def _validate_config(self, config: StackConfig) -> None:
        """Validate the stack configuration."""
        required_fields = [
            'stack_name', 'environment', 'ecr_registry', 'ecr_repository',
            'project_name', 'database_name', 'sns_topic_name', 'aws_secret_name',
            'sf_data_bucket', 'uh_environment', 'template_mode'
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

        valid_template_modes = ['PRODUCTION', 'PRESENT']
        if config.template_mode not in valid_template_modes:
            raise ValueError(f"Template mode '{config.template_mode}' must be one of: {valid_template_modes}")

        # Environment variables:
        # biobayb_uh_publisher: AWS_SECRET_NAME, UH_DLQ_URL, UH_SNS_TOPIC_ARN
        # biobayb_uh_uploader: SF_DATA_BUCKET, UH_ENVIRONMENT, AWS_SECRET_NAME
        # Both functions have access to AWS Secrets Manager secret 'prod/biobayb/uh/keys'

        # lambda configs (environment variables will be updated after resource creation)
        self.lambda_config = {
            "biobayb_uh_uploader": {
                "description": "UltraHuman data uploader Lambda function",
                "handler": "ultrahuman.uh_uploader.lambda_handler",
                "timeout": Duration.minutes(15),
                "memory_size": 1024,
                "environment": {
                    "UH_ENVIRONMENT": self.config.uh_environment,
                    "SF_DATA_BUCKET": self.config.sf_data_bucket,
                    "SF_DATABASE_NAME": self.config.database_name,
                    "AWS_SECRET_NAME": self.config.aws_secret_name
                }
            },
            "biobayb_uh_publisher": {
                "description": "UltraHuman SNS publisher Lambda function",
                "handler": "ultrahuman.uh_publisher.lambda_handler",
                "timeout": Duration.minutes(10),
                "memory_size": 1024,
                "environment": {
                    "AWS_SECRET_NAME": self.config.aws_secret_name,
                    "UH_ENVIRONMENT": self.config.uh_environment
                }
            },
            "biobayb_uh_template_generator": {
                "description": "UltraHuman weekly report template generator Lambda function",
                "handler": "ultrahuman.templates.lambda_handler",
                "timeout": Duration.minutes(10),
                "memory_size": 1024,
                "environment": {
                    "AWS_SECRET_NAME": self.config.aws_secret_name,
                    "TEMPLATE_MODE": self.config.template_mode,
                    "SF_DATA_BUCKET": self.config.sf_data_bucket
                }
            },
            "biobayb_uh_jwt_coordinator": {
                "description": "UltraHuman JWT coordinator Lambda function for Step Functions",
                "handler": "ultrahuman.uh_jwt_coordinator.lambda_handler",
                "timeout": Duration.minutes(10),
                "memory_size": 1024,
                "environment": {
                    "AWS_SECRET_NAME": self.config.aws_secret_name,
                    "JWT_BATCH_SIZE": "10"
                }
            },
            "biobayb_uh_jwt_worker": {
                "description": "UltraHuman JWT worker Lambda function for Step Functions",
                "handler": "ultrahuman.uh_jwt_worker.lambda_handler",
                "timeout": Duration.minutes(15),
                "memory_size": 2048,
                "environment": {
                    "AWS_SECRET_NAME": self.config.aws_secret_name,
                    "JWT_EXPIRATION_DAYS": self.config.jwt_expiration_days,
                    "SF_DATA_BUCKET": self.config.sf_data_bucket
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

        # Create Step Functions state machine (after Lambda functions)
        self.create_stepfunctions_resources()
        
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
                "s3:GetBucketLocation",

                # Athena permissions for query execution
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:StopQueryExecution",
                "athena:GetWorkGroup",
                "athena:ListQueryExecutions",
                "athena:ListNamedQueries",
                "athena:ListTableMetadata",
                "athena:GetTableMetadata",

                # Glue permissions for data catalog
                "glue:GetDatabase",
                "glue:CreateDatabase",
                "glue:GetTable",
                "glue:GetPartitions",
                "glue:GetPartition",
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
                "sqs:GetQueueAttributes",
                
                # Step Functions permissions
                "states:StartExecution",
                "states:DescribeExecution",
                "states:DescribeStateMachine",
                "states:ListExecutions"
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
                log_group_name=f"/aws/lambda/{self.config.project_name}_{function_name}",
                retention=logs.RetentionDays.INFINITE,
                removal_policy=RemovalPolicy.RETAIN
            )

            # Create Lambda function - all functions use the same "shared" ECR image
            lambda_function = lambda_.DockerImageFunction(
                self, f"{self.config.project_name}_{function_name}_Lambda",
                function_name=f"{self.config.project_name}_{function_name}_Lambda",
                description=config["description"],
                code=lambda_.DockerImageCode.from_ecr(
                    repository=self.ecr_repo,
                    tag_or_digest="shared",  # All functions use the same shared image
                    cmd=[config["handler"]]  # Override the handler via CMD
                ),
                role=self.lambda_execution_role,
                timeout=config["timeout"],
                memory_size=config["memory_size"],
                environment=config["environment"],
                log_group=log_group,
                
                # Container-specific settings
                architecture=lambda_.Architecture.X86_64,
                
                # Dead letter queue for failed executions
                dead_letter_queue=self.uh_dlq,
                
                # Retry configuration
                retry_attempts=2
            )

            self.lambda_functions[function_name] = lambda_function

            # Add Function URL for template generator
            if function_name == "biobayb_uh_template_generator":
                function_url = lambda_function.add_function_url(
                    auth_type=lambda_.FunctionUrlAuthType.NONE,
                    cors=lambda_.FunctionUrlCorsOptions(
                        allowed_origins=["*"],
                        allowed_methods=[lambda_.HttpMethod.GET],
                        allowed_headers=["*"]
                    )
                )
                
                # Output the Function URL
                cdk.CfnOutput(
                    self, f"{self.config.project_name}_{function_name}_FunctionURL",
                    value=function_url.url,
                    description=f"Function URL for {self.config.project_name}_{function_name} Lambda function"
                )

            # Output the Lambda function ARN
            cdk.CfnOutput(
                self, f"{self.config.project_name}_{function_name}_Lambda_ARN",
                value=lambda_function.function_arn,
                description=f"ARN for {self.config.project_name}_{function_name} Lambda function"
            )
        
        # Add dynamic environment variables after all resources are created
        self.update_lambda_environment_variables()

    def create_lambda_aliases(self) -> None:
        """Create Lambda aliases with provisioned concurrency pointing to published versions."""
        
        for function_name, lambda_function in self.lambda_functions.items():
            # Create a published version first
            version = lambda_.Version(
                self, f"{self.config.project_name}_{function_name}_Version",
                lambda_=lambda_function,
                description=f"Published version for {function_name}"
            )
            
            # Create alias pointing to the published version
            alias = lambda_.Alias(
                self, f"{self.config.project_name}_{function_name}_Alias",
                alias_name="LIVE",
                version=version,
                description=f"LIVE alias for {function_name}"
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
        if "biobayb_uh_publisher" in self.lambda_aliases:
            self.uh_data_collection_topic.grant_publish(
                self.lambda_aliases["biobayb_uh_publisher"]
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

    def create_stepfunctions_resources(self) -> None:
        """Create Step Functions state machine for JWT generation."""
        
        # Create Step Functions execution role
        stepfunctions_role = iam.Role(
            self, f"{self.config.project_name}_StepFunctionsExecutionRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )
        
        # Add Lambda invoke permissions
        stepfunctions_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["lambda:InvokeFunction"],
                resources=[
                    self.lambda_functions["biobayb_uh_jwt_worker"].function_arn,
                    f"{self.lambda_functions['biobayb_uh_jwt_worker'].function_arn}:*"
                ]
            )
        )
        
        # Create worker Lambda task
        worker_task = stepfunctions_tasks.LambdaInvoke(
            self, "ProcessParticipantJWT",
            lambda_function=self.lambda_functions["biobayb_uh_jwt_worker"],
            input_path="$",
            result_path="$.result",
            retry_on_service_exceptions=True
        )
        
        # Create Map state for parallel processing
        map_state = stepfunctions.Map(
            self, "ProcessParticipants",
            items_path="$.participants",
            max_concurrency=10
        )
        
        # Create a Pass state to transform the input for each item
        transform_input = stepfunctions.Pass(
            self, "TransformInput",
            parameters={
                "participant_id.$": "$.participant_id",
                "start_date.$": "$$.Execution.Input.start_date",
                "end_date.$": "$$.Execution.Input.end_date", 
                "update_mdh": True
            }
        )
        
        # Chain the transform and worker task
        item_chain = transform_input.next(worker_task)
        
        # Set the item processor for the Map state
        map_state.item_processor(item_chain)
        
        # Create Pass state for success
        success_state = stepfunctions.Pass(
            self, "AllParticipantsProcessed",
            result=stepfunctions.Result.from_object({
                "status": "SUCCESS",
                "message": "All participants processed successfully"
            })
        )
        
        # Create the state machine definition
        definition = map_state.next(success_state)
        
        # Create the state machine
        self.jwt_state_machine = stepfunctions.StateMachine(
            self, f"{self.config.project_name}_JWTStateMachine",
            state_machine_name=f"{self.config.project_name}-jwt-generation",
            definition_body=stepfunctions.DefinitionBody.from_chainable(definition),
            role=stepfunctions_role,
            timeout=Duration.minutes(60),
            logs=stepfunctions.LogOptions(
                destination=logs.LogGroup(
                    self, f"{self.config.project_name}_StepFunctionsLogGroup",
                    log_group_name=f"/aws/stepfunctions/{self.config.project_name}-jwt-generation",
                    retention=logs.RetentionDays.INFINITE,
                    removal_policy=RemovalPolicy.RETAIN
                ),
                level=stepfunctions.LogLevel.ALL
            )
        )
        
        # Output the state machine ARN
        cdk.CfnOutput(
            self, f"{self.config.project_name}_JWTStateMachineARN",
            value=self.jwt_state_machine.state_machine_arn,
            description="ARN for UltraHuman JWT generation Step Functions state machine"
        )
        
        # Add Step Functions state machine ARN to coordinator
        if "biobayb_uh_jwt_coordinator" in self.lambda_functions:
            coordinator_lambda = self.lambda_functions["biobayb_uh_jwt_coordinator"]
            coordinator_lambda.add_environment("JWT_STATE_MACHINE_ARN", self.jwt_state_machine.state_machine_arn)

    def create_eventbridge_rules(self) -> None:
        """Create EventBridge rules for scheduled Lambda executions using aliases."""
        
        # Schedule for SNS publisher (runs daily at midnight AZ time UTC-7)
        if "biobayb_uh_publisher" in self.lambda_aliases:
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
                targets.LambdaFunction(self.lambda_aliases["biobayb_uh_publisher"])
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

        # Manual trigger capability for JWT coordinator  
        if "biobayb_uh_jwt_coordinator" in self.lambda_aliases:
            # This creates a custom event pattern that can be triggered manually
            jwt_coordinator_rule = events.Rule(
                self, f"{self.config.project_name}_UHJWTCoordinatorManualTriggerRule",
                description="Manual trigger for UltraHuman JWT coordinator",
                event_pattern=events.EventPattern(
                    source=["sensorfabric.manual"],
                    detail_type=["UltraHuman JWT Generation Request"]
                )
            )
            
            jwt_coordinator_rule.add_target(
                targets.LambdaFunction(self.lambda_aliases["biobayb_uh_jwt_coordinator"])
            )

    def update_lambda_environment_variables(self) -> None:
        """Update Lambda functions with dynamic environment variables."""
        
        # Add SNS topic ARN and DLQ URL to SNS publisher
        if "biobayb_uh_publisher" in self.lambda_functions:
            publisher_lambda = self.lambda_functions["biobayb_uh_publisher"]
            publisher_lambda.add_environment("UH_SNS_TOPIC_ARN", self.uh_data_collection_topic.topic_arn)
            publisher_lambda.add_environment("UH_DLQ_URL", self.uh_dlq.queue_url)

        # Add DLQ URL to uploader
        if "biobayb_uh_uploader" in self.lambda_functions:
            uploader_lambda = self.lambda_functions["biobayb_uh_uploader"]
            uploader_lambda.add_environment("UH_DLQ_URL", self.uh_dlq.queue_url)
        
