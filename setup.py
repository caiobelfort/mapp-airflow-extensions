from setuptools import setup, find_packages

setup(
    name="mapp-airflow-extensions",
    descriptions="Airflow extensions used my MateusApp Data Team",
    version='0.1a',
    author='Caio Belfort',
    author_email='caiobelfort90@gmail.com',
    packages=find_packages(exclude=['tests']),
    license='GPL',
    install_requires=['apache-airflow==1.10.3'],
    entry_points={
        'airflow.plugins': [
            'mapp = mapp:MappPlugin'
        ]
    }
)
