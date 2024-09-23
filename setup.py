from distutils.core import setup

setup(
    name='pymysqlpool',
    version='0.1',
    packages=['pymysqlpool'],
    url='',
    license='MIT',
    author='Zillion Technologies',
    author_email='vdokku@zilliontechnologies.com',
    requires=['pymysql', 'pandas'],
    description='MySQL connection pool utility.'
)