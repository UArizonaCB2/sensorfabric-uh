#!/usr/bin/env python3

"""
Used only for local testing to render the reporting templates.
Do not include this in the production lambda build.
"""

from flask import Flask, request
from templates import lambda_handler

app = Flask(__name__)

@app.route('/')
def index():
    event = {
        't': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwYXJ0aWNpcGFudF9pZCI6IkJCLTI4NDQtMDAxNSIsInN0YXJ0X2RhdGUiOiIyMDI1LTA4LTAzIiwiZW5kX2RhdGUiOiIyMDI1LTA4LTEwIiwiaWF0IjoxNzU0ODA5MjU1LCJleHAiOjE3NTc0MDEyNTV9.XbNpKpBHf8e28veT4nGhFCqt4NU7XoSxv8ejMvWyV3E',
    }
    html = lambda_handler(event, None)
    return html['body']

if __name__ == '__main__':
    app.run(debug=True)
