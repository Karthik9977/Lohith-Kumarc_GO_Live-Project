# Asynchronous Event Handling Using Microservices and Kafka

## Overview

This project demonstrates asynchronous event handling using Go microservices and Kafka. It includes:

- **Producer Service**: Sends events to Kafka topics.
- **Consumer Service**: Listens to Kafka topics and processes events.
- **Kafka Setup**: Managed via Docker Compose.
- **JSON Schemas**: Define the structure of different event types.

## Prerequisites

- Docker and Docker Compose installed.
- Go installed (version 1.20 or higher).

## Setup Instructions

1. **Start Kafka and Zookeeper**:

   Navigate to the project root directory and run:

   ```bash
   docker-compose up -d
