
# Entregable-2
Carga de datos en Amazon Redshift.

- [How can I install `entregable-2`?](#how-can-i-install-entregable-2)
- [How can I run this ?](#how-can-i-run-this?)

# How can I install `entregable-2`?
- Create a new a virtual environment with Python3. [here](https://realpython.com/intro-to-pyenv/) is a good example using Pyenv.
- After activate your virtual environment, you can run the command below to install all the required dependencies:
```
pip install -r requirements.txt
```
# How can I run this?
- To run this you'll need an .env file at the root of the repo containing the following Redshift DB data:
```
DB_NAME=
HOST=
PORT=
USERNAME=
PASSW=
```
- Now you can run the command below and you should be good to go!
```
python retrieve_bitcoin_data.py
```