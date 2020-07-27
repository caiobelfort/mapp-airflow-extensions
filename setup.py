from setuptools import setup, find_packages

setup(
    name="mapp-airflow-extensions",
    descriptions="Airflow extensions used my MateusApp Data Team",
    version='0.1a',
    author='Caio Belfort',
    author_email='caiobelfort90@gmail.com',
    packages=find_packages(exclude=['tests']),
    license='GPL',
    install_requires=['apache-airflow==1.10.11'],
    entry_points={
        'airflow.plugins': [
            'mapp_gcs = mapp.operators.gcs_operators:MappGCSPlugin',
            'mapp_datavault = mapp.operators.dv_operators:MappDvPlugin',
            'mapp_mssql_to_gcs = mapp.operators.mssql_operators:MappMSSQLToGCSPlugin'
        ]
    }
)
