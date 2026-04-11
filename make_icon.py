from pathlib import Path

try:
    from PIL import Image
except Exception as ex:
    raise SystemExit(f"Install pillow first: pip install pillow\n{ex}")

base = Path(__file__).resolve().parent
png = base / "logo.png"
ico = base / "app.ico"

if not png.exists():
    raise SystemExit("logo.png not found in project root.")

img = Image.open(png).convert("RGBA")
# ICO prefers square sizes
sizes = [(256, 256), (128, 128), (64, 64), (48, 48), (32, 32), (16, 16)]
img.save(ico, format="ICO", sizes=sizes)
print(f"Created: {ico}")
