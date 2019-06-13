from setuptools import setup, find_packages

setup(
    name='curw',
    version='2.0.0-snapshot',
    packages=find_packages(exclude=['scratch_area']),
    url='http://www.curwsl.org',
    license='Apache 2.0',
    author='hasitha dhananjaya',
    author_email='hasitha.10@cse.mrt.ac.lk',
    description='Models being developed at the Center for URban Water, Sri Lanka',
    include_package_data=True,
    requires=['apache-airflow', 'shapely', 'joblib', 'netCDF4', 'matplotlib', 'imageio', 'scipy', 'geopandas'],
    zip_safe=False
    # install_requires=['shapely', 'joblib', 'netCDF4', 'matplotlib', 'imageio', 'numpy', 'pandas', 'scipy']
)
