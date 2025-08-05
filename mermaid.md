graph TB
      %% External Systems
      EB[EventBridge Rule Daily at 7:00 AM UTC]
      UH_API[UltraHuman API]
      MDH[(MDH Database)]
      USER[User/Browser]

      %% AWS Services
      ECR[ECR Repository uh-biobayb]
      SM[Secrets Manager prod/biobayb/uh/keys]
      CW[CloudWatch Logs]
      S3[S3 Bucket]

      %% SNS/SQS
      SNS[SNS Topic mdh_uh_sync]
      DLQ[SQS Dead Letter Queue biobayb_uh_undeliverable]

      %% Lambda Functions
      PUBLISHER[Lambda: biobayb_uh_publisher Fetches participants & publishes SNS]
      UPLOADER[Lambda: biobayb_uh_uploader Collects & uploads UH data]
      TEMPLATE[Lambda: biobayb_uh_template_generator Generates weekly reports]

      %% Data Flow
      EB -->|"Triggers daily"| PUBLISHER
      PUBLISHER -->|"Queries participant data"| MDH
      PUBLISHER -->|"Publishes participant info"| SNS
      SNS -->|"Triggers with participant data"| UPLOADER
      UPLOADER -->|"Fetches health data"| UH_API
      UPLOADER -->|"Stores parquet files"| S3
      UPLOADER -->|"Updates sync timestamps"| MDH
      UPLOADER -->|"Queries participant data"| MDH
      %% Template Generation (Function URL)
      USER -->|"HTTP request with token"| TEMPLATE
      TEMPLATE -->|"Reads data"| S3
      TEMPLATE -->|"Returns report"| USER

      %% Infrastructure Dependencies
      ECR -.->|"Shared Docker image"| PUBLISHER
      ECR -.->|"Shared Docker image"| UPLOADER
      ECR -.->|"Shared Docker image"| TEMPLATE

      SM -.->|"API credentials"| PUBLISHER
      SM -.->|"API credentials"| UPLOADER
      SM -.->|"API credentials"| TEMPLATE

      %% Error Handling
      PUBLISHER -->|"Failed messages"| DLQ
      UPLOADER -->|"Failed processing"| DLQ

      %% Logging
      PUBLISHER -.->|"Logs"| CW
      UPLOADER -.->|"Logs"| CW
      TEMPLATE -.->|"Logs"| CW

      %% Styling
      classDef lambda fill:#ff9900,stroke:#000,stroke-width:2px,color:#000
      classDef storage fill:#3498db,stroke:#000,stroke-width:2px,color:#fff
      classDef messaging fill:#e74c3c,stroke:#000,stroke-width:2px,color:#fff
      classDef external fill:#2ecc71,stroke:#000,stroke-width:2px,color:#000
      classDef infra fill:#9b59b6,stroke:#000,stroke-width:2px,color:#fff

      class PUBLISHER,UPLOADER,TEMPLATE lambda
      class S3,MDH,ECR storage
      class SNS,DLQ messaging
      class EB,UH_API,USER external
      class SM,CW infra