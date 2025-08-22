
本地测试

pipenv --python ~/.pyenv/versions/3.8.10/bin/python
ENV=DOCKER pipenv run python genenv.py
pipenv install
pipenv run python -m init_db
docker build -f Dockerfile -t your_name/jobmarket-database:0.0.1 .
docker push your_name/jobmarket-database:0.0.1eat