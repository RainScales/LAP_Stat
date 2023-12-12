FROM python:3.11.1
ENV APPLICATION_SERVICE=/app
# set work directory
RUN mkdir -p $APPLICATION_SERVICE

# where the code lives
WORKDIR $APPLICATION_SERVICE

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH `pwd`

# install dependencies
RUN mkdir $APPLICATION_SERVICE/data
COPY ./data $APPLICATION_SERVICE/data

COPY pyproject.toml ./
RUN pip install poetry && \
    pip install -U pip && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev

## copy project
COPY . $APPLICATION_SERVICE
CMD streamlit run streamlit_app.py