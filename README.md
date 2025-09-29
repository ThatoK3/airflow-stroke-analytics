Airflow Stroke Analytics Project
This project sets up an Airflow pipeline that runs Spark analytics on stroke prediction data and caches the results in PostgreSQL for Superset dashboards.


ls
README.md  config  dags  docker-compose.yml  dotenv.notes  init  logs  notes  scripts

Features
Airflow DAG that runs every 5 minutes

Spark analytics processing Avro data from S3

PostgreSQL cache for fast querying

Superset integration for data visualization

Docker-based setup for easy deployment

Prerequisites
Docker and Docker Compose

AWS credentials with S3 access

At least 8GB RAM available for containers

Quick Start
1. Clone and Setup

# Create project directory
mkdir airflow-stroke-analytics
cd airflow-stroke-analytics

# Create the directory structure
mkdir -p config init dags scripts logs

# Create all the files from the code above
# (Copy each file content to its respective location)
2. Configure Environment
Edit the .env file with your AWS credentials:


AWS_ACCESS_KEY_ID=your_actual_aws_access_key
AWS_SECRET_ACCESS_KEY=your_actual_aws_secret_key
3. Start the Stack

# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f airflow-webserver
4. Wait for Services to Initialize
Wait 2-3 minutes for all services to start completely. Check:


# Check if Airflow webserver is ready
curl http://localhost:8080

# Check if Superset is ready  
curl http://localhost:8088
5. Access the Services
Airflow: http://localhost:8080 (admin/admin)

Superset: http://localhost:8088 (admin/admin)

6. Set up Superset Database Connection
Open Superset at http://localhost:8088

Login with admin/admin

Go to Data → Databases

Click + Database

Select PostgreSQL

Connection string:


postgresql://analytics_user:analytics_pass@postgres:5432/analytics_db
Test connection and save

7. Monitor the Airflow DAG
Go to Airflow at http://localhost:8080

Find the stroke_analytics_cache DAG

Trigger it manually or wait for the scheduled run (every 5 minutes)

Monitor the logs for any issues


Analytics Tables Created
The pipeline creates these cache tables in PostgreSQL:

stroke_analytics_op_counts - Operation type counts

stroke_analytics_risk_distribution - Risk category percentages

stroke_analytics_probability_bins - Probability distribution

stroke_analytics_risk_by_gender - Risk breakdown by gender

stroke_analytics_risk_by_age_group - Risk by age categories

stroke_analytics_province_hotspots - Regional risk analysis

stroke_analytics_hypertension_heart_correlation - Medical condition correlations

stroke_analytics_bmi_vs_risk - BMI risk analysis

stroke_analytics_glucose_risk_bands - Glucose level correlations
