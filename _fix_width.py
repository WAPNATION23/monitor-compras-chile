"""Revert width='stretch' -> use_container_width=True in dashboard.py only."""
import pathlib

f = pathlib.Path("dashboard.py")
txt = f.read_text(encoding="utf-8")
count1 = txt.count("width='stretch'")
count2 = txt.count("width='content'")
txt = txt.replace("width='stretch'", "use_container_width=True")
txt = txt.replace("width='content'", "use_container_width=False")
f.write_text(txt, encoding="utf-8")
print(f"Replaced {count1} stretch + {count2} content")
