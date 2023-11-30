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
COPY pyproject.toml ./
RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev

## copy project
COPY . $APPLICATION_SERVICE
CMD streamlit run streamlit_app.py