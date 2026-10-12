import pytest

import utils


def _fs_is_case_insensitive(path) -> bool:
    probe = path / "CaseProbe"
    probe.write_text("x")
    return (path / "caseprobe").exists()


@pytest.fixture
def ci_dir(tmp_path, monkeypatch):
    if not _fs_is_case_insensitive(tmp_path):
        pytest.skip("filesystem is case-sensitive")
    monkeypatch.setattr(utils, "CASE_INSENSITIVE_FS", True)
    return tmp_path


def test_should_add_suffix_when_other_case_file_exists(ci_dir):
    (ci_dir / "photo.jpg").write_text("first")

    result = utils.avoid_case_collision(str(ci_dir / "Photo.jpg"))

    assert result == str(ci_dir / "Photo_1.jpg")


def test_should_keep_path_when_same_file_already_downloaded(ci_dir):
    (ci_dir / "Photo.jpg").write_text("mine")

    result = utils.avoid_case_collision(str(ci_dir / "Photo.jpg"))

    assert result == str(ci_dir / "Photo.jpg")


def test_should_reuse_own_suffixed_file_when_resuming(ci_dir):
    (ci_dir / "photo.jpg").write_text("first")
    (ci_dir / "Photo_1.jpg").write_text("second")

    result = utils.avoid_case_collision(str(ci_dir / "Photo.jpg"))

    assert result == str(ci_dir / "Photo_1.jpg")


def test_should_keep_path_when_file_is_absent(ci_dir):
    result = utils.avoid_case_collision(str(ci_dir / "new.jpg"))

    assert result == str(ci_dir / "new.jpg")


@pytest.mark.parametrize("remote, expected", [
    ("disk:/a/../../etc/passwd", "a/_/_/etc/passwd"),
    ("disk:/Фото/x:y?.jpg", "Фото/x_y_.jpg"),
    ("/CON/file. ", "_CON/file"),
])
def test_should_produce_safe_relative_path_when_remote_path_is_hostile(remote, expected):
    assert utils.sanitize_remote_path(remote) == expected
