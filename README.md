# Installing Apache Kafka on Windows

1. Download Kafka: [here](https://kafka.apache.org/downloads)
2. Extract Kafka: After downloading Kafka, extract the zip file to a directory of your choice (e.g., `C:\kafka`)
3. Open terminal and direct to kafka (e.g., `C:\kafka`)
4. Set Environment Variables: Add the bin directory of your Kafka installation to the `PATH` environment variable.
   This will allow you to run Kafka commands from the terminal.

-   Right-click on This PC → Properties → Advanced system settings → Environment Variables.
-   Under System Variables, find the Path variable, select it, and click `Edit`.
-   Add the path to the Kafka bin directory (e.g.,`C:\kafka\bin`) and click `OK`.

# Starting Zookeeper

-   Open another terminal and navigate to the Kafka directory (e.g., `C:\kafka`).
-   Run the following command to start Zookeeper: `.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties`

# Starting Kafka Server

-   Open another terminal and navigate to the Kafka directory (e.g., `C:\kafka`).
-   Run the following command to start Zookeeper: `.\bin\windows\kafka-server-start.bat .\config\server.properties`

# Running the Consumer With NodeJS

1.  `npm install`
2.  Set the topic in the `.env`
3.  `npm run start`

# How To Force Delete Kafka Log Folder on Windows

Open terminal:

-   `del /f C:\kafka\tmp\kafka-logs`
