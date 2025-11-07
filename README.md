# Spark Kafka Cassandra Streaming Pipeline

## Table of Contents
1. [About the Project](#about-the-project)  
2. [Architecture](#architecture)  
3. [Technologies Used](#technologies-used)  
4. [Getting Started](#getting-started)  

---

### About the Project
This project is a **real-time data streaming pipeline** that fetches user data from an API, streams it via **Kafka**, processes it with **Spark Structured Streaming**, and stores it in **Cassandra**. It also includes an **Airflow DAG** to automate the streaming tasks.  

---

### Architecture
![Architecture Diagram](https://github.com/user-attachments/assets/2a4c919a-0555-4d72-a9bc-03229ab76217)


- **Airflow**: Automates data fetching from the API.  
- **Kafka**: Streams the data in real-time.  
- **Spark Streaming**: Processes and transforms incoming data.  
- **Cassandra**: Persists the processed data.  

---

### Technologies Used
- Python  
- Apache Spark  
- Apache Kafka  
- Apache Cassandra  
- Apache Airflow  

---

### Getting Started
1. **Clone the repository**
   ```bash
   git clone https://github.com/ChaimaAlaoui/End-to-End-Data-Pipeline-with-Airflow-Kafka-and-Spark.git
   
2. **Navigate to the project directory:**
   ```bash
   cd End-to-End-Data-Pipeline-with-Airflow-Kafka-and-Spark

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt

4. **Start all services using Docker Compose:**
   ```bash
   docker-compose up
   
