## Project Data Flow Diagram and Architecture Overview

### Data Flow Diagram (Mermaid)

```mermaid
flowchart TD
    A[Raw Data in Snowflake] --> B[Load Raw Data into Pandas]
    B --> C[Data Quality Checks]
    C --> D[Data Transformations]
    D --> E[Feature Engineering Pipeline]
    E --> F[Reshape & Prepare Features]
    F --> G[Write Features to Iceberg Table in Snowflake]
    G --> H[Register Feature Metadata in Feature Catalog]
    H --> I[Feature Catalog Table in Snowflake]
    J[Monitoring & Logging] -.-> B
    J -.-> C
    J -.-> D
    J -.-> E
    J -.-> G
    J -.-> H
