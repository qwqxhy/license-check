# -*- coding: utf-8 -*-

from licensedb import LicenseDB


def match(p_license, c_license, p_dual=False, c_dual=False):

    ignore_license = (
        "free-unknown",
        "patent-disclaimer",
        "proprietary-license",
        "proprietary",
        "public-domain",
        "public-domain-disclaimer",
        "other-copyleft",
        "other-permissive",
        "trademark-notice",
        "unknown",
        "unknown-license-reference",
        "unknown-spdx",
        "ofl-1.0",
        "ofl-1.0-no-rfn",
        "ofl-1.0-rfn",
        "ofl-1.1",
        "ofl-1.1-no-rfn",
        "ofl-1.1-rfn",
    )

    gpl_pattern = (
        ("gpl-3.0", "gpl-3.0-plus"),
        ("gpl-3.0", "gpl-2.0-plus"),
        ("gpl-2.0", "gpl-2.0-plus"),
        ("gpl-3.0-plus", "gpl-3.0"),
        ("gpl-3.0-plus", "gpl-2.0-plus"),
        ("gpl-2.0-plus", "gpl-2.0"),
        ("gpl-2.0-plus", "gpl-3.0"), # TODO 使用有问题, 条款没冲突
        ("gpl-2.0-plus", "gpl-3.0-plus"), # TODO 使用有问题, 条款没冲突
    )

    conflict = []

    dual = False

    for pk in [n for n in p_license if n not in ignore_license and 'cc-by' not in n]:
        for ck in [m for m in c_license if m not in ignore_license and 'cc-by' not in m]:
            if pk == ck or (pk, ck) in gpl_pattern:
                continue
            conflict_term = match_two_license(pk, ck)
            if conflict_term:
                conflict.append({'plicense': pk, 'clicense': ck, 'term': conflict_term})
            else:
                if p_dual or c_dual:
                    conflict = []
                    dual = True
                    break
        if dual:
            break

    return conflict


def match_two_license(plicense, clicense):
    ldb = LicenseDB()

    conflict_term = []
    # p0 c1
    p0c1_item = ['disclose_source', 'network_use_disclose', 'same_license']
    # p1 c0
    p1c0_item = ['commercial_use', 'distribution', 'modification', 'private_use',
                 'patent_use', 'trademark_use']
    # p1 c1
    p1c1_item = ['same_license']

    en_names = ['commercial_use', 'distribution', 'modification', 'private_use', 'patent_use',
                'trademark_use', 'disclose_source', 'license_and_copyright_notice', 'same_license',
                'state_changes', 'network_use_disclose', 'liability', 'warranty']

    cn_names = ['商业用途', '分发', '修改', '私人使用', '专利使用',
                '商标使用', '代码开源', '许可和版权声明', '相同许可证',
                '修改记录', '网络服务开源', '免责', '担保']

    # must_items = ['license_copyright_notice', 'state_changes']
    
    pl = ldb.get_license_term_by_key(plicense)
    cl = ldb.get_license_term_by_key(clicense)

    if pl and cl:
        for item in p0c1_item:
            if pl[item] == 0 and cl[item] == 1:
                index = en_names.index(item)
                name = cn_names[index]
                conflict_term.append(name)
        for item in p1c0_item:
            if pl[item] == 1 and cl[item] == 0:
                index = en_names.index(item)
                name = cn_names[index]
                conflict_term.append(name)
        for item in p1c1_item:
            if pl[item] == 1 and cl[item] == 1:
                index = en_names.index(item)
                name = cn_names[index]
                conflict_term.append(name)
    
    return conflict_term