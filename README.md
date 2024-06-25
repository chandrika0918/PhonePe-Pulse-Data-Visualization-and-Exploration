# PhonePe Pulse Data Visualization and Exploration

This repository contains a Streamlit application that visualizes and analyzes data from PhonePe Pulse. The app provides insights into digital transactions and user patterns on the PhonePe platform across India.

## Table of Contents
- [Introduction](#introduction)
- [Features](#features)
- [Data Source](#data-source)
- [Installation](#installation)
- [Usage](#usage)
- [Setup and Database Connection](#setup-and-database-connection)
- [Streamlit App](#streamlit-app)
  - [Analysis Section](#analysis-section)
    - [Aggregated Data Analysis](#aggregated-data-analysis)
    - [Map Data Analysis](#map-data-analysis)
    - [Top Data Analysis](#top-data-analysis)
- [Queries](#queries)
  - [Query Functions](#query-functions)
- [Visualizations](#visualizations)
  - [Plotting with Plotly](#plotting-with-plotly)
- [License](#license)
- [Summary](#summary)

## Introduction

PhonePe Pulse is an Indian digital payments and financial technology platform. This application visualizes and explores data from PhonePe Pulse, providing insights into transaction amounts, user registrations, and more, across different states and districts in India.

## Features

- View top and bottom states/districts by transaction amount and registered users.
- Explore data by different categories such as transactions, users, and insurance.
- Interactive visualizations using Plotly and choropleth maps.
- Insights based on yearly and quarterly data.

## Data Source

The data used in this application is sourced from the PhonePe Pulse database, which contains information on transactions and user registrations across India.

## Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/your-username/phonepe-pulse-visualization.git
2. Navigate to the project directory:
    ```sh
    cd phonepe-pulse-visualization
## Usage
- streamlit run phonepe_dashboard.py
- Open your browser and go to http://localhost:8501 to view the application.

## Setup and Database Connection
The application connects to a MySQL database named phonepe_data using mysql.connector. The connection details (host, user, password, and database name) are specified in the mydb variable.

## Streamlit App
The Streamlit application is divided into four main sections: About, Home, Analysis, and Insights. Users can navigate through these sections using a sidebar menu.

## Analysis Section
The Analysis section provides a detailed view of the data based on different categories like AGGREGATED, MAP, and TOP. Each tab allows users to filter data by method (TRANSACTION, USER, INSURANCE), year, quarter, and other relevant parameters.

## Aggregated Data Analysis
- Transaction data is visualized using bar charts and maps.
- User data shows registered users and transaction counts.
- Insurance data displays insurance transaction counts and amounts.
## Map Data Analysis
- Visualizes transaction counts and amounts by state.
- Registered user data by state.
- Insurance data by state.
## Top Data Analysis
- Shows top transactions and registered users by state.
## Queries
The application defines several functions to retrieve data based on different queries. Each function executes a SQL query and returns the result as a pandas DataFrame with a custom index.

# Query Functions
1. ques_1(): Top 10 states by total transaction amount.
2. ques_2(): Bottom 10 states by total transaction amount.
3. ques_3(): Top 10 districts by total registered users.
4. ques_4(): Bottom 10 districts by total registered users.
5. ques_5(): Top 10 districts by transaction amount.
6. ques_6(): Bottom 10 districts by transaction amount.
7. ques_7(): Top 10 districts by transaction count.
8. ques_8(): Bottom 10 districts by transaction count.
9. ques_9(): Transaction types by total transaction amount.
10. ques_10(): Top 10 brands by transaction count.
## Visualizations
The app provides various visualizations including:

## Plotting with Plotly
The application uses Plotly for creating bar charts and maps. GeoJSON files are used to plot the data on the map of India.

- Bar charts showing transaction amounts and counts by state.
- Choropleth maps for a geographical view of transaction data.
- Comparison of different brands based on transaction counts.
## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Summary
This Streamlit app is a comprehensive tool for visualizing PhonePe's transaction data across India. It provides users with the ability to explore various metrics and insights through interactive charts and maps, facilitating a better understanding of digital payment trends in the country. 
