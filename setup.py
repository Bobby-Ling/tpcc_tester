from setuptools import setup, find_packages
import tpcc_tester
from os import path
this_directory = path.abspath(path.dirname(__file__))
long_description = None
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name='tpcc_tester', # 包名称
      packages=['tpcc_tester'], # 需要处理的包目录
      version='0.0.1', # 版本
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python', 'Intended Audience :: Developers',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: 3.10',
          'Programming Language :: Python :: 3.11',
          'Programming Language :: Python :: 3.12'
      ],
      install_requires=['pymysql'],
      entry_points={'console_scripts': ['tpcc_tester=tpcc_tester.runner:main']},
      package_data={'': ['*.json']},
      auth='Bobby Ling', # 作者
      author_email='bobby-ling@outlook.com', # 作者邮箱
      description='tpcc tester', # 介绍
      long_description=long_description, # 长介绍，在pypi项目页显示
      long_description_content_type='text/markdown', # 长介绍使用的类型
      url='https://github.com/Bobby-Ling/tpcc_tester.git', # 包主页
      license='MIT', # 协议
      keywords='tpcc tester') # 关键字 搜索用