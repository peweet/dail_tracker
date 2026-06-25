import requests
from bs4 import BeautifulSoup

H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

r = requests.get("https://meetings.southdublin.ie/Home/Agenda/2753", headers=H, timeout=40)
soup = BeautifulSoup(r.text, "html.parser")

# Look for elements with class names that group items
# Print a slice of raw HTML around "Headed Items"
html = r.text
idx = html.find("Headed Items")
print(html[idx - 200: idx + 1800])
