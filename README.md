<p align="center">
  <h1 align="center">🐘 HDFS Client</h1>
  <p align="center">
    <strong>Spring Boot REST wrapper that creates Hive tables and loads CSV files over JDBC.</strong>
  </p>
  <p align="center">
    <img src="https://img.shields.io/badge/Java-8-ed8b00?style=for-the-badge&logo=openjdk&logoColor=white" alt="Java">
    <img src="https://img.shields.io/badge/Spring_Boot-2.0-6db33f?style=for-the-badge&logo=springboot&logoColor=white" alt="Spring_Boot">
    <img src="https://img.shields.io/badge/Apache_Hive-JDBC-fdee21?style=for-the-badge&logo=apachehive&logoColor=white" alt="Apache_Hive">
    <img src="https://img.shields.io/badge/Maven-build-c71a36?style=for-the-badge&logo=apachemaven&logoColor=white" alt="Maven">
    <img src="https://img.shields.io/github/last-commit/pratiks360/hdfs-client?style=for-the-badge&label=Last+Commit" alt="Last Commit">
  </p>
</p>

---

## ✨ What Is This?

**HDFS Client** lets other services create Hive tables and bulk-load CSV data through two HTTP calls instead of talking to Hive directly.

## 🔌 Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/hdfs/hive/create` | Create a Hive table from a `HiveSchema` JSON body |
| `POST` | `/api/hdfs/hive/upload-csv-file` | Upload a CSV (multipart) into a table |

## 🧩 How It Works

```
┌────────┐       ┌────────────────┐     ┌─────────────┐  hive-jdbc  ┌──────┐
│ Client │ ────► │ HiveController │ ──► │ HiveService │ ──────────► │ Hive │
└────────┘       └────────────────┘     │ + HiveRepo  │             └──────┘
                                        └─────────────┘
```

## 🚀 Quick Start

**Prerequisites:** JDK 8+, Maven, a reachable HiveServer2.

```bash
git clone https://github.com/pratiks360/hdfs-client.git
cd hdfs-client
mvn spring-boot:run
```

Set the Hive JDBC URL in `src/main/resources/application.properties`.

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Framework** | Spring Boot 2.0.3 (web) |
| **Data** | Hive JDBC 2.3.4 |
| **Build** | Maven |

---

<p align="center">
  <sub>Built with ❤️ for big-data plumbing.</sub>
</p>
