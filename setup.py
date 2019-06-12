from setuptools import setup

setup(
    name="mapp-airflow-extensions",
    version='0.1a',
    author='Caio Belfort',
    author_email='caiobelfort90@gmail.com',
    packages=['mapp'],
    license='GPL',
    install_requires=['apache-airflow==1.10.3'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Software Development',
        'Intended Audience :: Developers',
        "Programming Language :: Python :: 3.6"
    ],
    entry_points={
        "airflow.plugins": [
            "mapp_plugin = mapp.mapp_plugin:MappPlugin"
        ]
    }
)
