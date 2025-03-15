import os
import sys

# Define file paths
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#File paths
movies_path = "Data files\\movies.dat"
ratings_path = "Data files\\ratings.dat"
users_path = "Data files\\users.dat"
