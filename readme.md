# Ad Bidding Service & Test Suite

This repository contains a simple test procedure Ad Bidding Service.

## Prerequisites

Before you begin, ensure you have the following installed:
* [Go](https://go.dev/doc/install/) (version 1.18 or newer)

## 🚀 Setup & Running

Getting the entire environment running is simple.

1.  **Clone the Repository**
    ```bash
    git clone <your-repository-url>
    cd <your-repository-folder>
    ```
2.  **Download Dependencies for the Test Tool**
    Before running the test tool for the first time, download its dependencies:
    ```bash
    go mod tidy
    ```

## 📂 Project Structure

* `main.go`: A powerful Go-based CLI tool for interacting with and testing the API.
* **(Your Go Service Code)**: The source code for the `hiring-software-engineer-task` application itself.

## ⚙️ Using the CLI Test Tool

All commands are run from your terminal in the project's root directory.

### Create a Line Item
This command creates a new ad line item. The arguments must be in the exact order shown.

```bash
# Usage: go run main.go create <name> <advertiser_id> <bid> <budget> <placement>
go run main.go create "My Test Ad" "adv777" 5.5 8000 "homepage_top"
```

### Get a Winning Ad
This command fetches the winning ad for a given set of targeting criteria.

```bash
# Usage: go run main.go get-ad <placement> <category> <keyword>
go run main.go get-ad homepage_top electronics summer
```

### Run Targeted Ad Logic Tests
This test runs through the first 5 line items from the test data, uses their targeting to make a request,
and verifies that the correct ad is returned. and instead of 5 it can be 20.

```bash
go run main.go ad-test
```

### Run Validation Tests
This test checks the validation rules on the "create line item" endpoint by sending both valid and invalid data.

```bash
go run main.go validation-test
```

### Run End-to-End Tracking Test
This is a full pipeline test. It sends 15 tracking events to the API, waits for them to be processed through Kafka, and then connects directly to ClickHouse to verify that all 15 events were successfully saved.

```bash
go run main.go e2e-tracking-test
```