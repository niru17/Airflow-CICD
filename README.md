# ✈️ Flight Booking Data Pipeline with CI/CD using Airflow and Spark

This project demonstrates a complete end-to-end data pipeline for flight booking data using **Apache Airflow**, **Apache Spark**, and **GitHub Actions** for CI/CD. It showcases how to automate data ingestion, transformation, and orchestration in a production-grade workflow.

---

## 📦 Project Structure

```
Flight-CICD-Airflow/
├── airflow_job/              # Airflow DAG and scripts
├── spark_job/                # PySpark job for data processing
├── variables/                # Airflow variable definitions
├── .github/workflows/        # GitHub Actions for CI/CD
├── flight_booking.csv        # Sample input data
├── Dockerfile / requirements.txt (if applicable)
└── README.md
```

---

## 🚀 Features

- 🛫 Ingests flight booking data from CSV
- ⚙️ Processes data using Apache Spark
- 📅 Orchestrates tasks with Apache Airflow
- 🔁 Automates CI/CD deployment via GitHub Actions
- 📊 Outputs curated flight data

---

## 🛠️ Technologies Used

- **Apache Airflow**
- **Apache Spark (PySpark)**
- **GitHub Actions**
- **Docker (optional)**
- **GCP / Local environment**

---

## 🔧 How It Works

1. **Airflow DAG** schedules the job.
2. **CSV file (`flight_booking.csv`)** is used as the raw input.
3. **Spark job** reads the file and transforms the data.
4. **DAG tasks** manage ingestion and processing.
5. **GitHub Actions** validate and deploy DAG changes automatically.

---

## ⚙️ CI/CD Workflow

Located at `.github/workflows/ci-cd.yaml`, the GitHub Actions pipeline:

- Lints Python files
- Checks DAG integrity
- Optionally builds and pushes Docker image

---

## 🧪 Sample DAG Logic

- `start`: Dummy start task
- `upload_to_gcs`: Optional GCS step (if using cloud)
- `spark_process`: Triggers the PySpark job
- `validate_output`: Ensures result validity

---

## 📂 Input Data

- `flight_booking.csv`  
  Contains records with fields like `flight_id`, `origin`, `destination`, `status`, etc.

---

## 📬 Author

**Niranjana Subramanian**  
_Data Engineer | Cloud & Data Enthusiast_

---

## 📝 License

This project is for educational and demonstration purposes.
