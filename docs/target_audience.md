# Target Audience

This document describes who should use this project and the specific use cases they would benefit from.

---

## Who Should Use This Project

### 1. Data Engineers

**Profile**: Engineers building and maintaining data pipelines, especially CDC-based pipelines.

**Use Cases**:
- Learning CDC patterns with Debezium + Kafka + Databricks
- Building production CDC pipelines for their organization
- Understanding medallion architecture (Bronze/Silver/Gold)
- Implementing Data Vault 2.0 for enterprise data warehousing

**What they get**:
- Complete working reference implementation
- Best practices for CDC capture and processing
- Patterns for handling schema evolution
- Template for their own projects

---

### 2. Databricks Users

**Profile**: Data analysts, engineers, and architects using Databricks for data processing.

**Use Cases**:
- Setting up structured streaming from Kafka
- Implementing MERGE patterns for deduplication
- Using Unity Catalog for governance
- Building medallion lakehouse architectures

**What they get**:
- Working examples of Databricks patterns
- Notebook templates for common operations
- Integration with dbt for transformations
- Job orchestration examples

---

### 3. Data Vault Practitioners

**Profile**: Data architects and engineers implementing Data Vault 2.0 methodology.

**Use Cases**:
- Understanding Data Vault patterns in Databricks
- Learning hash key strategies (SHA-256)
- Implementing satellite change detection (DIFF_HASH)
- Building PIT and Bridge tables

**What they get**:
- Complete Data Vault implementation reference
- Hash key generation patterns
- Satellite append-only strategies
- Business Vault examples

---

### 4. Solution Architects

**Profile**: Architects evaluating CDC approaches and lakehouse technologies for their organization.

**Use Cases**:
- Evaluating Debezium vs other CDC tools
- Comparing lakehouse vs traditional data warehouse
- Designing multi-layer data architectures
- Planning GDPR-compliant data pipelines

**What they get**:
- Architecture patterns and trade-offs
- Technology comparison reference
- Production considerations for each layer
- Governance and compliance patterns

---

### 5. Data Governance Teams

**Profile**: Teams responsible for data quality, compliance, and metadata management.

**Use Cases**:
- Implementing data quality monitoring
- Setting up PII tracking and tagging
- Planning GDPR erasure workflows
- Building data lineage and audit trails

**What they get**:
- DQ check templates and implementations
- PII catalog helpers
- Crypto-shredding erasure pipeline
- Schema drift detection patterns

---

### 6. DevOps / Platform Engineers

**Profile**: Engineers responsible for deploying and maintaining data infrastructure.

**Use Cases**:
- Setting up local development environments
- Deploying Databricks jobs via API
- Managing secrets and credentials
- Building CI/CD pipelines for data

**What they get**:
- Docker-based local stack
- Job deployment automation
- Secret management patterns
- Infrastructure-as-code examples

---

### 7. ML Engineers

**Profile**: Engineers building ML models that need clean, historized training data.

**Use Cases**:
- Accessing clean, deduplicated data from Silver/Gold
- Using historical data for feature engineering
- Building time-series features from Vault
- Ensuring data quality for model training

**What they get**:
- Clean, tested data layers
- Historical data access patterns
- Data quality guarantees
- Feature store foundation

---

## Use Case Examples

### Use Case 1: New CDC Pipeline

**Scenario**: Company wants to capture changes from legacy PostgreSQL database.

**Solution**: Use this project as reference to:
1. Set up Debezium + Kafka
2. Create Bronze/Silver layers
3. Deploy to Databricks

**Time savings**: 2-4 weeks of architecture design

---

### Use Case 2: Data Vault Implementation

**Scenario**: Organization wants to implement Data Vault 2.0 for audit compliance.

**Solution**: Use this project to:
1. Understand DV2 patterns
2. Generate vault from existing schema
3. Implement PIT/Bridge tables

**Time savings**: 1-2 months of design work

---

### Use Case 3: GDPR Compliance

**Scenario**: Company needs GDPR-compliant data pipeline with erasure capability.

**Solution**: Use this project to:
1. Set up PII tracking
2. Implement encryption
3. Build erasure pipeline

**Time savings**: 3-4 months of compliance work

---

### Use Case 4: Lakehouse Migration

**Scenario**: Moving from traditional data warehouse to Databricks lakehouse.

**Solution**: Use this project to:
1. Understand medallion layers
2. Implement Bronze/Silver/Gold
3. Add dbt transformations

**Time savings**: 2-3 months of migration planning

---

## Who Might Not Need This Project

### Not for Beginners
If you're new to data engineering entirely, start with simpler tutorials before diving into CDC + Data Vault + Databricks.

### Not for One-Off ETL
If you just need a one-time data load, this project is overkill. Consider simpler tools.

### Not for Non-PostgreSQL Sources
This project targets PostgreSQL specifically. Debezium supports other sources, but configuration would differ.

---

## Learning Path Recommendations

### Beginner
1. Start with `quickstart.md` to run the pipeline
2. Read `architecture.md` to understand concepts
3. Explore `components.md` for implementation details

### Intermediate
1. Understand each layer's purpose
2. Modify configs for different tables
3. Add new Gold models

### Advanced
1. Extend DV2 generator for new schemas
2. Customize agent system for your needs
3. Add compliance workflows

---

## Support and Contribution

For questions or contributions:
- Check `troubleshooting.md` for common issues
- Review `ROADMAP.md` for future work
- See `AGENTS.md` for AI assistant guidance