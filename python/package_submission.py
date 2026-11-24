"""Create the Moodle submission from a source-only allowlist, without datasets."""

from pathlib import Path
import tempfile
import zipfile


ROOT = Path(__file__).resolve().parent.parent


def submission_files():
    patterns = ["README.md", ".gitignore", ".gitattributes", "docker-compose.yml", "start.sh", "run.sh",
                "python/*.py", "python/README.md", "python/requirements.txt", "python/.run/*.xml",
                "python/docker-compose.yml", "python/*.sh", "python/pyproject.toml", "python/uv.lock", "python/.gitignore",
                "python/dashboard/*.html", "python/dashboard/*.css", "python/dashboard/*.js",
                "java/pom.xml", "java/Dockerfile", "java/.dockerignore", "java/run_jobs.ps1", "java/README.md",
                "java/.run/*.xml", "java/src/main/**/*.java",
                "docs/*.py", "docs/dataset-metadata.json", "docs/requirements.txt", "output/pdf/project-documentation.pdf",
                "reports/offset-comparison.json", "reports/offset-comparison.md"]
    return sorted({path for pattern in patterns for path in ROOT.glob(pattern) if path.is_file()})


def main():
    files = submission_files()
    required = [ROOT / "java/pom.xml", ROOT / "python/dashboard.py",
                ROOT / "output/pdf/project-documentation.pdf"]
    for path in required:
        if path not in files:
            raise SystemExit(f"Missing required submission file: {path}")
    directory = ROOT / "output/submission"
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / "hw1-231035.zip"
    with tempfile.NamedTemporaryFile(dir=directory, suffix=".zip", delete=False) as temporary:
        temporary_path = Path(temporary.name)
    try:
        with zipfile.ZipFile(temporary_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in files:
                name = Path("hw1-231035") / path.relative_to(ROOT)
                if path.suffix == ".sh":
                    info = zipfile.ZipInfo.from_file(path, name.as_posix())
                    info.create_system = 3
                    info.external_attr = 0o100755 << 16
                    archive.writestr(info, path.read_bytes(), compress_type=zipfile.ZIP_DEFLATED)
                else:
                    archive.write(path, name)
        temporary_path.replace(destination)
    finally:
        temporary_path.unlink(missing_ok=True)
    print(f"Created {destination} ({len(files)} files, {destination.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
