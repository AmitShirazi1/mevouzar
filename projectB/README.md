# Project 2: Demographic Data Analysis and Streaming Visualization

## Overview

This project aims to analyze demographic data and visualize viewing behaviors using Apache Spark. It involves reading and processing demographic information, applying clustering techniques to categorize households, and analyzing streaming data from Kafka to understand viewing patterns.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Setup](#setup)
3. [Data Sources](#data-sources)
4. [Features](#features)
5. [How to Run](#how-to-run)
6. [Code Structure](#code-structure)
7. [Contributors](#contributors)

## Prerequisites

- Python 3.x
- Apache Spark 3.x
- PySpark
- Kafka
- Pandas
- Jupyter Notebook (for the `.ipynb` file)

## Setup

1. **Install dependencies**: Ensure you have all necessary libraries installed. You can use pip or conda to manage your Python packages.

   ```bash
   pip install pyspark pandas
   ```

2. **Apache Kafka**: Make sure Kafka is running and accessible at the specified address in the code.

3. **Spark Configuration**: Adjust Spark configurations as necessary based on your environment.

## Data Sources

- **Demographic Data**: The project reads demographic data from a specified path in Parquet format.
- **Viewing Data**: Streaming data is received from a Kafka topic (`viewstream`), and static viewing data is pulled from another topic (`viewstatic`).

## Features

- **Data Loading and Caching**: Efficient loading of large datasets using Spark's caching mechanism.
- **Feature Extraction and Normalization**: Extracts key features from demographic data and normalizes them for analysis.
- **One-Hot Encoding**: Converts categorical variables into a format suitable for machine learning.
- **Dimensionality Reduction**: Uses PCA to reduce feature dimensions for better visualization.
- **K-Means Clustering**: Applies K-Means clustering to categorize households based on features.
- **Dynamic Data Analysis**: Analyzes streaming data to calculate the viewing behavior of households over time.

## How to Run

1. Open the Jupyter Notebook file `PROJECT2_WET_322620873_314779166.ipynb`.
2. Follow the steps in the notebook to load and process the demographic data.
3. Execute the code cells to run the clustering and streaming analysis.
4. Adjust any parameters (like Kafka server settings) as necessary for your setup.

## Code Structure

- `preprocessing.py`: Functions for preprocessing data before analysis.
- `optimization.py`: Functions to optimize the performance of the Spark job.
- `main.py`: The main script for running the analysis.
- `inference.py`: Functions for making predictions based on the processed data.
- `generate_comp_tagged.py`: Functions for generating comparative tagged data.
