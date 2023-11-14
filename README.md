<h1 align="center">Cinema-backend of 4 microservices by Q1nfo</h1>



**Source Code**: [github.com/q1nfo/django-admin](https://github.com/Q1nfo/django-admin)

**1 PART - admin panel in django** : [Django-admin](https://github.com/Q1nfo/django-admin) \
**2 PART - core functionality manager on FastApi** : [Fastapi-movie]() \
**3 PART - service updating data from the database in ElasticSearch** : [Db-to-Elastic]() \
**4 PART - auth service on Flask**: [Flask-auth]()

---

<!--intro-start-->

Cinema project, which consists of 3 microservices interacting with each other via docker-compose

This is 3 services that serve to constantly update data from the main 
database, which is presented in the form of a post-regression to the 
database to implement the full-text search function, which in this 
project is represented by elasticsearch. For this purpose, this project 
has implemented a caroutine system that transmits data at a given period of 
time through the pipes until walking to the final goal

To get started, jump to the [installation](#installation) section or keep reading to learn more about the included
features.
<!--intro-end-->

<!--readme-start-->

## ✨ Features

### 🩺 Code Quality, Formatting, and Linting Tools

* [Flake8](https://flake8.pycqa.org/) - Tool For Style Guide Enforcement
* [pre-commit](https://pre-commit.com/) - Git hook scripts are useful for identifying simple issues before submission to code review.
* [isort](https://pycqa.github.io/isort/) - isort is a Python utility / library to sort imports alphabetically, and automatically separated into sections and by type.


## Installation

### Requirements

Before proceeding make sure you have installed [Docker](https://docs.docker.com/engine/installation/) . Docker with Docker Compose is used for local development.

### Manual Installation

    $ gh repo clone Q1nfo/project
    $ mv project example
    $ cd example

    touch .env

    pip install -r requirements


<!--readme-end-->