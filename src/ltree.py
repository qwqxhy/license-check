# -*- coding: utf-8 -*-
import json
import os
import pickle
import traceback

from treelib import Tree, Node
from lconflict import match
from licensedb import LicenseDB

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
    )


class LTree():

    def __init__(self):
        self.tree = Tree()
        self.display_tree = {}
        self.license = []
        self.license_file = ''
        self.dual_license = False
        self.file_number = 0
        self.has_license = 0
        self.license_kind = 0
        self.license_total = 0
        self.license_count = []
        self.license_conflict = []
        self.__license_list = []
        self.__count = {}


    def output(self):
        print('license:', self.license)
        print('license_file:', self.license_file)
        print('dual_license:', self.dual_license)
        print('license_kind:', self.license_kind)
        print('license_total:', self.license_total)
        print('license_count:', self.license_count)
        print('license_conflict:', self.license_conflict)
    
    def get_result(self):
        return {'license:': self.license, 'file_number': self.file_number, 
        'license_file:': self.license_file,'dual_license:': self.dual_license, 
        'has_license:':self.has_license, 'no_license:': self.file_number-self.has_license, 
        'license_kind:': self.license_kind, 'license_total:': self.license_total, 'license_count:': self.license_count,
        'license_conflict:': self.license_conflict, 'license_tree:': self.display_tree}

    def save(self, savepath_pickle):
        with open(savepath_pickle, 'wb') as file:
            pickle.dump(self.tree, file)

    def load(self, loadpath_pickle):
        with open(loadpath_pickle, 'rb') as file:
            self.tree = pickle.load(file)
    
    def save_display_tree(self, savepath_json):
        with open(savepath_json, 'w') as file:
            json.dump(self.display_tree, file, indent=2)
    
    def build_display_tree(self):
        self.display_tree = self._get_display_tree(self.tree.root)

    def build(self, files):

        success = True
        message = '0'
        try:
            self._get_project_license(files)
            # 添加根节点
            root_path = 'project'
            root_license = [n if n!='gpl-1.0-plus' else 'gpl-3.0-plus' for n in self.license]
            self.__license_list.extend(root_license)
            root_node = Node(identifier=root_path, tag=0,
                            data={'license': root_license, 'file': self.license_file, 'dual': self.dual_license, 'has_conflict': False, 'is_guide': False})
            self.tree.add_node(root_node)
            
            for file in files:
                # 添加节点
                if file['type'] == 'directory':
                    index = files.index(file)
                    folder_license, folder_license_file, dual_license = self._get_folder_license(files, index)
                    self.__license_list.extend(folder_license)
                    folder_node = Node(identifier=file['path'], tag=1,
                                    data={'license': folder_license, 'file': folder_license_file, 'dual': dual_license, 'has_conflict': False, 'is_guide': False})
                    parent_node_id = self._get_parent_id(file['path'])
                    while not self.tree.contains(parent_node_id):
                        parent_node_id = self._get_parent_id(parent_node_id)
                    self.tree.add_node(folder_node, parent=parent_node_id)

                if file['type'] == 'file':
                    self.file_number += 1
                    if not file['licenses']:
                        continue
                    
                    self.has_license += 1

                    if file['base_name'].lower() == 'license' or file['extension'].lower() == '.license' or file['is_legal'] or file['is_readme'] or file['is_manifest']:
                        continue
                    
                    file_license, dual_license = self._get_file_license(file['licenses'])
                    self.__license_list.extend(file_license)
                    file_node = Node(identifier=file['path'], tag=2,
                                    data={'license': file_license, 'file': file['name'], 'dual': dual_license, 'has_conflict': False, 'is_guide': False})
                    parent_node_id = self._get_parent_id(file['path'])
                    while not self.tree.contains(parent_node_id):
                        parent_node_id = self._get_parent_id(parent_node_id)
                    self.tree.add_node(file_node, parent=parent_node_id)
            
            flag = 1
            remove = 0
            while flag:
                leaves = self.tree.leaves()
                for leaf in  leaves:
                    identifier = leaf.identifier
                    if identifier == self.tree.root:
                        flag = 0
                        break
                    if leaf.data['license']:
                        continue
                    else:
                        self.tree.remove_node(identifier)
                        remove += 1
                if remove == 0:
                    flag = 0
                remove = 0

        except Exception as e:
            success = False
            message = 'license tree build error:\nException: {}'.format(traceback.format_exc())
        return success, message
    
    def detect(self):
        success = True
        message = '0'
        self.count()
        try:
            self.check()
        except Exception as e:
            success = False
            message = 'conflict check error:\nException: {}'.format(traceback.format_exc())
        try:
            self.build_display_tree()
        except Exception as e:
            success = False
            message = 'display tree build error:\nException: {}'.format(traceback.format_exc())
        return success, message
    
    def count(self):
        self.__license_list = [x for x in self.__license_list if x not in ignore_license]
        self.__license_list = ['gpl-3.0-plus' if x == 'gpl-1.0-plus' else x for x in self.__license_list]
        self.license_total = len(self.__license_list)
        unique_license = set(self.__license_list)
        self.license_kind = len(unique_license)
        for i in unique_license:
            self.__count[i] = self.__license_list.count(i)
        self.__count = dict(sorted(self.__count.items(), key=lambda x: x[1], reverse=True))
        # mongodb存储key包含(.)报错(可以 check_keys=False 强制存储，不建议)
        ldb = LicenseDB()
        self.license_count = [{'name': key, 'category':ldb.get_license_category_by_key(key), 'count': value} for key, value in self.__count.items()]
    
    def check(self):
        root_node = self.tree.get_node(self.tree.root)
        root_id = root_node.identifier
        if root_node.data["license"]:
            for child_id in self.tree.expand_tree(nid=root_id):
                if child_id == root_id:
                    continue
                child_node = self.tree.get_node(child_id)
                parent_license = root_node.data["license"]
                parent_dual = root_node.data["dual"]
                child_license = child_node.data["license"]
                child_dual = child_node.data["dual"]
                con = match(parent_license, child_license, parent_dual, child_dual)
                if con:
                    self.license_conflict.append({"pfile": root_node.data['file'], "cfile": child_id, "conflict": con})
                    root_node.data['has_conflict'] = True
                    child_node.data['has_conflict'] = True
                    child_node.data['is_guide'] = True
                    self._add_guide(child_node)

        for node in [n for n in self.tree.all_nodes() if n.tag == 1]:
            parent_id = node.identifier
            for child_id in self.tree.expand_tree(nid=parent_id):
                if child_id == parent_id:
                    continue
                child_node = self.tree.get_node(child_id)
                parent_license = node.data["license"]
                parent_dual = node.data["dual"]
                child_license = child_node.data["license"]
                child_dual = child_node.data["dual"]
                con = match(parent_license, child_license, parent_dual, child_dual)
                if con:
                    self.license_conflict.append({"pfile": node.data['file'], "cfile": child_id, "conflict": con})
                    node.data['has_conflict'] = True
                    child_node.data['has_conflict'] = True
                    child_node.data['is_guide'] = True
                    self._add_guide(child_node)
    
    def _add_guide(self, node):
        node_id = node.identifier
        while node_id != self.tree.root:
            parent_node = self.tree.parent(node_id)
            parent_node.data['is_guide'] = True
            node_id = parent_node.identifier


    def _get_project_license(self, files):
        for file in files:
            if file['is_top_level']:
                if not file['type'] == 'file':
                    continue
                if file['base_name'].lower() == 'license' or file['extension'].lower() == '.license':
                    if 'licenses' in file and file['licenses']:
                        if self.license_file:
                            self.license_file = self.license_file + ', ' + file['path']
                        else:
                            self.license_file = file['path']
                        for i in file['licenses']:
                            if i['key'] not in ignore_license:
                                self.license.append(i['key'])
                            if 'OR' in i['matched_rule']['license_expression'] or 'AND' in i['matched_rule']['license_expression']:
                                self.dual_license = True
            else:
                break
        self.license = [x for x in self.license if x not in ignore_license]
        self.license = ['gpl-3.0-plus' if x == 'gpl-1.0-plus' else x for x in self.license]
        self.license = list(set(self.license))
        if len(self.license) < 2:
            self.dual_license = False
        if len(self.license) >= 2:
            self.dual_license = True


    def _get_folder_license(self, files, index):
        dual = False
        license = []
        folder_license_file = ''
        folder_path = files[index]['path']
        for file in files:
            if file['type'] != 'file':
                continue
            if self._get_parent_id(file['path']) != folder_path:
                continue
            if file['base_name'].lower() == 'license' or file['extension'].lower() == '.license':
                if file['licenses'] and not file['is_top_level']:
                    if folder_license_file:
                        folder_license_file = folder_license_file + ', ' + file['path']
                    else:
                        folder_license_file = file['path']
                    for i in file['licenses']:
                        if i['key'] not in ignore_license:
                            key = i['key'] if i['key']!='gpl-1.0-plus' else 'gpl-3.0-plus'
                            if key not in license:
                                license.append(key)
                        if 'OR' in i['matched_rule']['license_expression'] or 'AND' in i['matched_rule']['license_expression']:
                            dual = True

        license = [x for x in license if x not in ignore_license]
        license = ['gpl-3.0-plus' if x == 'gpl-1.0-plus' else x for x in license]
        license = list(set(license))
        if len(license) < 2:
            dual = False
        if len(license) >= 2:
            dual = True
        return license, folder_license_file, dual
    
    def _get_file_license(self, licenses):
        dual = False
        license = []
        for i in licenses:
            if i['key'] not in ignore_license:
                key = i['key'] if i['key']!='gpl-1.0-plus' else 'gpl-3.0-plus'
                if key not in license:
                    license.append(key)
            if 'OR' in i['matched_rule']['license_expression'] or 'AND' in i['matched_rule']['license_expression']:
                dual = True
        license = [x for x in license if x not in ignore_license]
        license = ['gpl-3.0-plus' if x == 'gpl-1.0-plus' else x for x in license]
        license = list(set(license))
        if len(license) < 2:
            dual = False
        if len(license) >= 2:
            dual = True
        return license, dual
    
    def _get_parent_id(self, path):
        folder = os.path.split(path)[0]
        if not folder:
            folder = 'project'
        return folder

    def _get_display_tree(self, nid):
        node = self.tree.get_node(nid)
        if node.is_leaf():
            tag = node.tag
            if tag == 0:
                name = node.identifier
                vulue = node.data['license']
            else:
                name = os.path.split(node.identifier)[-1]
                vulue = node.data['license']
            has_conflict = node.data['has_conflict']
            is_guide = node.data['is_guide']
            collapsed = False
            return {'name': name, 'value': vulue, 'has_conflict': has_conflict, 'is_guide': is_guide, 'collapsed': collapsed, 'children': []}
        else:
            children = []
            child_id_list = self.tree.is_branch(nid)
            for child_id in child_id_list:
                children.append(self._get_display_tree(child_id))
            node = self.tree.get_node(nid)
            tag = node.tag
            if tag == 0:
                name = node.identifier
                vulue = node.data['license']
            else:
                name = os.path.split(node.identifier)[-1]
                vulue = node.data['license']
            has_conflict = node.data['has_conflict']
            is_guide = node.data['is_guide']
            if self.tree.depth(node) == 0:
                collapsed = True
            else:
                collapsed = False
            return {'name': name, 'value': vulue, 'has_conflict': has_conflict, 'is_guide': is_guide, 'collapsed': collapsed, 'children': children}
