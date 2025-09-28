from typing import Optional
from bs4 import BeautifulSoup
import requests

def parse_html(response: Optional[requests.Response]) -> Optional[BeautifulSoup]:

    return BeautifulSoup(response.text, "html.parser")
