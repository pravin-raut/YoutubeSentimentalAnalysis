from setuptools import setup, find_packages

setup(
    name="KafkaYoutube",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'textblob',
        'kafka-python',
        'google-auth',  
        'google-api-python-client'
    ]
)
