# -*- coding: utf-8 -*-
from scancode import cli
import os
import traceback

from ltree import LTree

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
TREE_DIR =os.path.join(CURRENT_DIR, 'tree')
DISPLAYED_TREE_DIR =os.path.join(CURRENT_DIR, 'display_tree')
RESULT_DIR =os.path.join(CURRENT_DIR, 'result')

ignores_pattern = (
    '.git/', '.github/', '.idea/',
    '*.pdf', '*.jpg', '*.jpeg', '*.png', '*.gif', '*.bmp',
    '*.mp3', '*.mp4', '*.avi', '*.WAV', '*.MOV', '*.mid', '*.cda', '*.rmvb',
)


def _run_scancode(path):
    # Keep only options required by ltree field dependencies:
    # - info: path/type/base_name/extension/is_top_level etc.
    # - classify: is_legal/is_readme/is_manifest.
    # - license: licenses.
    # Also pass explicit empty defaults for plugin options to avoid
    # Click Sentinel values crashing ScanCode plugins in programmatic mode.
    return cli.run_scan(
        path,
        license=True,
        info=True,
        classify=True,
        include=('*',),
        ignore=ignores_pattern,
        facet=(),
        license_policy=None,
        ignore_copyright_holder=(),
        ignore_author=(),
        strip_root=True,
        return_results=True,
        processes=0,
    )


def _collect_scan_errors(results):
    scan_errors = []
    headers = results.get("headers", []) if isinstance(results, dict) else []
    for header in headers:
        scan_errors.extend(header.get("errors") or [])
    return scan_errors


def license_check(codebase):
    """ 检测 代码 许可证合规性
    :param codebase: 待测代码目录
    :return: success: 检测成功(True)或失败(False)
             results: 检测结果 成功返回检测结果; 失败返回{}
             message: 检测成功 返回 '0'; 检测失败 返回失败的原因
    """
    success = True
    result = {}
    message = '0'
    path = codebase

    if not os.path.exists(codebase):
        message = 'path: {}: not exists'.format(codebase)
        success = False
        return success, result, message

    file_num = len(os.listdir(codebase))
    
    if file_num == 0:
        message = 'path: {}: is an empty directory'.format(codebase)
        success = False
        return success, result, message
    
    if file_num == 1:
        file = os.listdir(codebase)[0]
        if os.path.isdir(os.path.join(codebase, file)):
            path = os.path.join(codebase, file)

    try:
        rc, results = _run_scancode(path)
    except Exception:
        success = False
        message = 'scancode cli error: {}: \nException: {}'.format(codebase, traceback.format_exc())
        return success, result, message

    files = results.get("files", [])
    if not files:
        success = False
        message = 'scancode cli error: no file scan results'
        return success, result, message

    scan_errors = _collect_scan_errors(results)

    ltree = LTree()
    build_success, message = ltree.build(files)
    success = success and build_success
    if not build_success:
        return success, result, message

    detect_success, message = ltree.detect()
    success = success and detect_success
    if detect_success:
        result = ltree.get_result()
        if scan_errors:
            result["scan_errors"] = scan_errors
            result["scan_error_count"] = len(scan_errors)
            message = '0 (with scan errors)'
        else:
            message = '0'
    else:
        success = False
    
    return success, result, message
        

if __name__ == '__main__':
    import json
    path = '/work/test/abcdefxyz123456789abcdefxyz123456789'
    success, results, message = license_check(path)
    print(success, message)
    print(json.dumps(results))
