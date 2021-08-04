# astro
This is an ongoing project.


## Table of Contents

- [Introduction](#introduction)

- [Installation](#installation)

- [Data sources](#data-sources)

- [Instructions](#instructions)

- [Architecture](#architecture)

- [Next steps](#next-steps)

---

## Introduction
### Description
This is a repository that will contain all my personal projects related to astronomy.

### Objectives
- Learn astronomy in general;
- Learn how to analyze data from Earth-based and space telescopes;
- Discover a new planet using Machine Learning (ML) models.

### When?
It is an ongoing project, started on `April 10, 2021`.

### Visuals
![Live demo](core/assets/images/live_demo.png)


## Installation
To run the program and see a demo of the code, you need:
- To install the libraries below
- To download the datasets (see [Data sources](#data-sources) for information).

### Install the libraries
| Library          | Used to                                        |
| ---------------- | :----------------------------------------------|
| Numpy            | To handle Numpy arrays                         |
| Pandas           | To store and access info in a DataFrame        |
| Matplotlib       | To plot the data                               |
| Scikit-learn     | To preprocess dataset, perform machine learning|
| jupyter          | To see Exploratory Data Analysis and live demo |


Follow these instructions to install the required libraries: on terminal
1. Open your terminal;
2. cd to the directory where the `requirements.txt` file is located;
3. Create and activate your virtual environment.
4. Run the command: 
```pip3 install -r requirements.txt```

### Additional info
Note that I develop the source code on macOS Big Sur

## Data Sources
To train my machine learning model, I use the datasets from
- *Kepler*;
- *K2*;
- *TESS*.

You can download the public datasets here: 

Source:
...

## Instructions
### How to run the program
- Run `main.py` to start the program.
- Run `main_demo.ipynb` to see a live demo of the program.
Or
On your terminal:
1. Open your terminal;
2. cd to the directory where the `main.py` or `main_demo.ipynb` are
3. Activate your virtual environment.
4. Run the command:
- `python3 main.py` (to run the program)
- `jupyter notebook main_demo.ipynb` (to open the jupyter notebook) 

### Usage example
![Demo usage](core/assets/images/demo_usage.png)



## Architecture
The project is structured as follows:

```
astro
│   README.md               :explains the project
│   main.py                 :script to run in order to start the program
│   main_demo.ipynb         :jupyter notebook to see a demo of the program
│   requirements.txt        :packages to install to run the program
│   .gitignore              :files to ignore when pushing to the GitHub repository
│
└───core                    :directory contains all the core scripts of the program
│   │   __init__.py
│   │
│   └───assets              :contains the datasets, images and ML models
│       ├───data
│       ├───images
│       └───models
│   └───tutorials           :contains the tutorials I follow (mainly from lightkurve)
```

### Roadmap
- [x] Understand the ways to find planets
- [ ] Follow lightkurve tutorials
- [ ] Download the dataset
- [ ] Exploratory Data Analysis
- [ ] Clean dataset
- [ ] Prepare dataset for machine learning
- [ ] If labeled data, build ML models for predictive classification
- [ ] If no labeled data, build ML models for clustering
- [ ] Select the right performance metrics for your model
- [ ] Evaluate the distribution of datapoints and evaluate its influence in the model
- [ ] Tuning parameters of the model for better performance
- [ ] Identify if models underfit or overfit and use techniques to prevent it
- [ ] Select best model according to performance metrics
- [ ] Define the strengths and limitations of the model

Finally:
- [ ] Draw conclusions from data analysis and predictions
- [ ] Optimize code
- [ ] Prepare tech talk presentation (15 minutes + 5 minutes Q&A)


### Author(s) and acknowledgment
This project is carried out by **Van Frausum Derrick** 
from Theano 2.27 promotion at BeCode.

I would like to thank:
- my colleagues and coaches at BeCode for their help and guidance.


## Next steps
- Progress in roadmap
- Explore this idea: save lightcurves as images, build & train CNN model to classify as planet candidate, no planet, binary stars, ...
