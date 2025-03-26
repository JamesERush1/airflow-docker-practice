FROM apache/airflow:2.8.1-python3.10

# Copy your DAG and migration script
COPY dags/ /opt/airflow/dags/
# Install dependencies
USER airflow
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Switch back to airflow user
USER airflow
