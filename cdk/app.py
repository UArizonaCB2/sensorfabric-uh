#!/usr/bin/env python3

import aws_cdk as cdk
from sensorfabric_lambda_stack import SensorFabricLambdaStack, StackConfig

app = cdk.App()

# Get environment variables or use defaults
account = app.node.try_get_context("account") or "509812589231"
region = app.node.try_get_context("region") or "us-east-1"

# Get configuration from context or use defaults
config = StackConfig(
    stack_name=app.node.try_get_context("stack_name") or "Ultrahuman-Dev",
    environment=app.node.try_get_context("environment") or "dev",
    ecr_registry=app.node.try_get_context("ecr_registry") or "509812589231.dkr.ecr.us-east-1.amazonaws.com",
    ecr_repository=app.node.try_get_context("ecr_repository") or "uh-biobayb",
    project_name=app.node.try_get_context("project_name") or "uh-biobayb-dev",
    database_name=app.node.try_get_context("database_name") or "uh-biobayb-dev",
    sns_topic_name=app.node.try_get_context("sns_topic_name") or "mdh_uh_sync",
    aws_secret_name=app.node.try_get_context("aws_secret_name") or "prod/biobayb/uh/keys",
    sf_data_bucket=app.node.try_get_context("sf_data_bucket") or "uoa-biobayb-uh-dev",
    uh_environment=app.node.try_get_context("uh_environment") or "production"
)

prodConfig = StackConfig(
    stack_name="Ultrahuman-Prod",
    environment="production",
    ecr_registry="509812589231.dkr.ecr.us-east-1.amazonaws.com",
    ecr_repository="uh-biobayb",
    project_name="uh-biobayb-prod",
    database_name="uh-biobayb-prod",
    sns_topic_name="mdh_uh_sync_prod",
    aws_secret_name="prod/biobayb/uh-prod/prod-keys",
    sf_data_bucket="uoa-biobayb-uh-prod",
    uh_environment="production"
)

# TODO: add other stacks.
# recommend using the same ECR registry since we're not changing out the lambda code per stack
# prod_stack = StackConfig(
#     stack_name=app.node.try_get_context("stack_name") or "Ultrahuman-Prod",
#     environment=app.node.try_get_context("environment") or "prod",
#     ecr_registry=app.node.try_get_context("ecr_registry") or "509812589231.dkr.ecr.us-east-1.amazonaws.com",
#     ecr_repository=app.node.try_get_context("ecr_repository") or "uh-biobayb",
#     project_name=app.node.try_get_context("project_name") or "uh-biobayb-prod",
#     database_name=app.node.try_get_context("database_name") or "uh-biobayb-prod",
#     sns_topic_name=app.node.try_get_context("sns_topic_name") or "mdh_uh_sync",
#     aws_secret_name=app.node.try_get_context("aws_secret_name") or "prod/biobayb/uh-prod/keys",
#     sf_data_bucket=app.node.try_get_context("sf_data_bucket") or "uoa-biobayb-uh-prod",
#     uh_environment=app.node.try_get_context("uh_environment") or "production"
# )

# Create the Lambda stack using the stack_name as construct_id
SensorFabricLambdaStack(
    app,
    config.stack_name,
    config=config,
    env=cdk.Environment(account=account, region=region),
    description=f"SensorFabric Staging - {config.stack_name} deployed via Docker containers"
)

SensorFabricLambdaStack(
    app,
    prodConfig.stack_name,
    config=prodConfig,
    env=cdk.Environment(account=account, region=region),
    description=f"SensorFabric Production - {prodConfig.stack_name} deployed via Docker containers"
)

app.synth()
