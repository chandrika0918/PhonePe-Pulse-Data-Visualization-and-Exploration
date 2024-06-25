# PhonePe-Pulse-Data-Visualization-and-Exploration
This application is designed to provide insights and visualizations based on PhonePe's transaction data across various states in India. Below is a detailed explanation of the features and functionalities included in the app
# Setup and Database Connection
The application connects to a MySQL database named phonepe_data using mysql.connector. The connection details (host, user, password, and database name) are specified in the mydb variable.
# Streamlit App
The Streamlit application is divided into three main sections: About, Home, Analysis, and Insights. Users can navigate through these sections using a sidebar menu.
# Analysis Section
The Analysis section provides a detailed view of the data based on different categories like AGGREGATED, MAP, and TOP. Each tab allows users to filter data by method (TRANSACTION, USER, INSURANCE), year, quarter, and other relevant parameters.

## Aggregated Data Analysis:
Transaction data is visualized using bar charts and maps.
User data shows registered users and transaction counts.
Insurance data displays insurance transaction counts and amounts.
## Map Data Analysis:
Visualizes transaction counts and amounts by state.
Registered user data by state.
Insurance data by state.
## Top Data Analysis:
Shows top transactions and registered users by state.
# Plotting with Plotly
The application uses Plotly for creating bar charts and maps. GeoJSON files are used to plot the data on the map of India.
# Summary
This Streamlit app is a comprehensive tool for visualizing PhonePe's transaction data across India. It provides users with the ability to explore various metrics and insights through interactive charts and maps, facilitating a better understanding of digital payment trends in the country.
