#  Copyright (C) 2017-2020 Red Hat, Inc. <http://www.redhat.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

# pylint: disable=too-many-statements, undefined-loop-variable
# pylint: disable=too-many-branches,too-many-locals,pointless-string-statement

from uuid import uuid4
from random import choice, randint
from time import sleep
from re import search
import glustolibs.gluster.constants as const
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
from glustolibs.gluster.glusterfile import (get_fattr, get_pathinfo,
                                            get_fattr_list, get_file_stat, get_pathinfo,
                                            file_exists, create_link_file,
                                            get_md5sum)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.brick_libs import (bring_bricks_online,
                                           bring_bricks_offline,
                                           are_bricks_offline,
                                           are_bricks_online)
from glustolibs.gluster.volume_libs import (get_subvols,
                                            volume_start)
from glustolibs.gluster.dht_test_utils import (
    find_new_hashed,
    find_hashed_subvol,
    create_brickobjectlist,
    find_nonhashed_subvol)
from glustolibs.gluster.mount_ops import mount_volume, umount_volume
from glustolibs.gluster.glusterfile import file_exists, move_file
from glustolibs.gluster.constants import FILETYPE_DIRS
from glustolibs.gluster.constants import \
    TEST_FILE_EXISTS_ON_HASHED_BRICKS as FILE_ON_HASHED_BRICKS
from glustolibs.gluster.constants import \
    TEST_LAYOUT_IS_COMPLETE as LAYOUT_IS_COMPLETE
from glustolibs.gluster.lib_utils import get_size_of_mountpoint
from glustolibs.gluster.glusterfile import calculate_hash
from glustolibs.gluster.brickdir import BrickDir
from glustolibs.gluster.lib_utils import append_string_to_file
from glustolibs.gluster.constants import TEST_LAYOUT_IS_COMPLETE

@runs_on([['distributed', 'distributed-arbiter', 'distributed-replicated', 'distributed-dispersed'],
          ['glusterfs']])
class TestDhtMultiCases(GlusterBaseClass):

    """
    Description: tests to check the dht layouts of files and directories,
                 along with their symlinks.
    """
   
    @classmethod
    def setUpClass(cls):

        # Calling GlusterBaseClass setUp
        cls.get_super_method(cls, 'setUpClass')()

        # Setup Volume and Mount Volume
        ret = cls.setup_volume_and_mount_volume(cls.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

        """
        self.client, self.m_point = (cls.mounts[0].client_system,
                                     cls.mounts[0].mountpoint)
        # Assign a variable for the first_client
        self.first_client = cls.mounts[0].client_system

        self.temp_folder = '/tmp/%s' % uuid4()
        """

    @classmethod
    def tearDownClass(cls):

        # Unmount and cleanup original volume
        ret = cls.unmount_volume_and_cleanup_volume(mounts=cls.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        cls.get_super_method(cls, 'tearDownClass')()

    destination_exists = False

    def copy_dir(self):
        """
        Description:
        This test creates a parent directory and subdirectories
        at mount point. After that it creates a copy of parent
        directory at mount point, first when destination
        directory is not there, and second sub-test creates a
        copy after creating destination directory for copying.
        In the first test, contents will be copied from one
        directory to another but in the second test case, entire
        directory will be copied to another directory along with
        the contents.Then it checks for correctness of layout
        and content of source and copied directory at all
        sub-vols.
        """

        g.log.info("creating multiple,multilevel directories")
        m_point = self.mounts[0].mountpoint
        fqpath = m_point + '/root_dir/test_dir{1..3}'
        client_ip = self.clients[0]
        flag = mkdir(client_ip, fqpath, True)
        self.assertTrue(flag, "Directory creation: failed")

        command = 'ls ' + m_point + '/root_dir'
        ret, out, _ = g.run(client_ip, command)
        self.assertEqual(ret, 0, "can't list the created directories")

        list_of_created_dirs = out.split('\n')
        flag = True
        for x_count in range(3):
            dir_name = 'test_dir%d' % (x_count + 1)
            if dir_name not in list_of_created_dirs:
                flag = False
        self.assertTrue(flag, "ls command didn't list all the directories")
        g.log.info("creation of multiple,multilevel directories created")

        g.log.info("creating files at different directory levels")
        command = 'touch ' + m_point + '/root_dir/test_file{1..5}'
        ret, _, _ = g.run(client_ip, command)
        self.assertEqual(ret, 0, "files not created")

        command = 'ls ' + m_point + '/root_dir'
        ret, out, _ = g.run(client_ip, command)
        self.assertEqual(ret, 0, "can't list the created directories")

        list_of_files_and_dirs = out.split('\n')
        flag = True
        for x_count in range(3):
            dir_name = 'test_dir%d' % (x_count + 1)
            if dir_name not in list_of_files_and_dirs:
                flag = False
        for x_count in range(5):
            file_name = 'test_file%d' % (x_count + 1)
            if file_name not in list_of_files_and_dirs:
                flag = False
        self.assertTrue(
            flag, "ls command didn't list all the directories and files")
        g.log.info("creation of files at multiple levels successful")

        if not self.destination_exists:
            destination_dir = 'root_dir_1'
        else:
            fqpath = m_point + '/new_dir'
            flag = mkdir(client_ip, fqpath, True)
            self.assertTrue(flag, "new_dir not created")
            destination_dir = 'new_dir/root_dir'

        g.log.info("performing layout checks for root_dir")
        flag = validate_files_in_dir(self.clients[0],
                                     m_point + '/root_dir',
                                     const.TEST_FILE_EXISTS_ON_HASHED_BRICKS)
        self.assertTrue(flag, "root directory not present on every brick")

        flag = validate_files_in_dir(self.clients[0],
                                     m_point + '/root_dir',
                                     test_type=(
                                         const.TEST_LAYOUT_IS_COMPLETE))
        self.assertTrue(flag, "layout of every directory is complete")
        g.log.info("every directory is present on every brick and layout "
                   "of each brick is correct")

        g.log.info("copying root_dir at the mount point")
        command = "cp -r " + m_point + '/root_dir ' + m_point \
                  + '/' + destination_dir
        ret, out, _ = g.run(client_ip, command)
        self.assertEqual(ret, 0, "directory was not copied")

        g.log.info("performing layout checks for copied directory")

        flag = validate_files_in_dir(self.clients[0],
                                     m_point + '/' + destination_dir,
                                     const.TEST_FILE_EXISTS_ON_HASHED_BRICKS)
        self.assertTrue(flag, "directories not present on every brick")

        flag = validate_files_in_dir(self.clients[0],
                                     m_point + '/' + destination_dir,
                                     test_type=(
                                         const.TEST_LAYOUT_IS_COMPLETE))
        self.assertTrue(flag, "layout of every directory is complete")
        g.log.info("verified: layouts correct")

        g.log.info("listing the copied directory")
        command = 'ls -A1 ' + m_point + '/' + destination_dir
        ret, out, _ = g.run(client_ip, command)
        self.assertIsNotNone(out, "copied directory not listed")

        g.log.info("copied directory listed")
        command = 'ls -A1 ' + m_point + '/root_dir'
        ret, out1, _ = g.run(client_ip, command)
        self.assertEqual(ret, 0, "details of root_dir not listed")

        command = 'ls -A1 ' + m_point + '/' + destination_dir
        ret, out2, _ = g.run(client_ip, command)
        self.assertEqual(ret, 0, "details of copied dir not listed")
        self.assertEqual(out1, out2,
                         "contents and attributes of original and "
                         "copied directory not same")
        g.log.info("the contents and attributes of copied directory "
                   "are same")

        g.log.info("listing the copied directory on all the subvolumes")
        brick_list = get_all_bricks(self.mnode, self.volname)
        for brick in brick_list:
            brick_tuple = brick.partition(':')
            brick_path = brick_tuple[2]
            host_addr = brick_tuple[0]

            command = 'ls -A1 ' + brick_path + '/' + destination_dir
            ret, out, _ = g.run(host_addr, command)
            self.assertIsNotNone(out,
                                 ("copied directory not listed on brick "
                                  "%s", brick))

            g.log.info("copied directory listed on brick %s", brick)
            command = 'ls -l --time-style=\'+\' ' + brick_path \
                      + '/root_dir/' + ' | grep ^d'
            ret, out1, _ = g.run(host_addr, command)
            self.assertEqual(ret, 0, "details of root_dir not listed on "
                                     "brick %s" % brick)

            command = 'ls -l --time-style=\'+\' ' + brick_path + '/' \
                      + destination_dir + '| grep ^d'
            ret, out2, _ = g.run(host_addr, command)
            self.assertEqual(ret, 0, "details of copied dir not listed on "
                                     "brick %s" % brick)
            self.assertEqual(out1, out2,
                             "contents and attributes of original and "
                             "copied directory not same on brick "
                             "%s" % brick)
            g.log.info("the contents and attributes of copied directory "
                       "are same on brick %s", brick)
        g.log.info("the copied directory is present on all the subvolumes")

    @classmethod
    def create_files(cls, host, root, files, content):
        """This method is responsible to create file structure by given
        sequence with the same content for all of the files
        Args:
            host (str): Remote host
            root (str): root file directory
            files (list|tuple): Sequence of file paths
            content (str): Textual file content for each of files.
        Returns:
            bool: True on success, False on error
        """
        for item in files:
            dir_name = root
            file_name = item
            if item.find('/') != -1:
                segments = item.split('/')
                folders_tree = "/".join(segments[:-1])
                file_name = segments[-1]
                dir_name = '{root}/{folders_tree}'.format(
                    root=root, folders_tree=folders_tree)
                mkdir(host, dir_name, parents=True)
            cmd = 'echo "{content}" > {root}/{file}'.format(root=dir_name,
                                                            file=file_name,
                                                            content=content)
            ret, _, _ = g.run(host, cmd)
            if ret != 0:
                g.log.error('Error on file creation %s', cmd)
                return False
        return True

    def test_create_directory(self):

        m_point = self.mounts[0].mountpoint
        command = 'mkdir -p ' + m_point + '/root_dir/test_dir{1..3}'
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "directory creation failed on %s"
                         % self.mounts[0].mountpoint)
        command = 'ls ' + m_point + '/root_dir'
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "ls failed on parent directory:root_dir")
        g.log.info("ls on parent directory: successful")

        command = 'touch ' + m_point + \
            '/root_dir/test_file{1..5} ' + m_point + \
            '/root_dir/test_dir{1..3}/test_file{1..5}'
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "File creation: failed")
        command = 'ls ' + m_point + '/root_dir'
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Failed to list the created directories")
        list_of_files_and_dirs = out.split('\n')
        flag = True
        for x_count in range(3):
            dir_name = 'test_dir%d' % (x_count+1)
            if dir_name not in list_of_files_and_dirs:
                flag = False
        for x_count in range(5):
            file_name = 'test_file%d' % (x_count+1)
            if file_name not in list_of_files_and_dirs:
                flag = False
        self.assertTrue(flag, "ls command didn't list all the "
                        "directories and files")
        g.log.info("Creation of files at multiple levels successful")

        command = 'cd ' + m_point + ';find root_dir -type d -print'
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Creation of directory list failed")
        list_of_all_dirs = out.split('\n')
        del list_of_all_dirs[-1]

        flag = validate_files_in_dir(self.clients[0],
                                     m_point + '/root_dir',
                                     test_type=const.TEST_LAYOUT_IS_COMPLETE)
        self.assertTrue(flag, "Layout has some holes or overlaps")
        g.log.info("Layout is completely set")

        brick_list = get_all_bricks(self.mnode, self.volname)
        for direc in list_of_all_dirs:
            list_of_gfid = []
            for brick in brick_list:
                # the partition function returns a tuple having 3 elements.
                # the host address, the character passed i.e. ':'
                # , and the brick path
                brick_tuple = brick.partition(':')
                brick_path = brick_tuple[2]
                gfid = get_fattr(brick_tuple[0], brick_path + '/' + direc,
                                 'trusted.gfid')
                list_of_gfid.append(gfid)
            flag = True
            for x_count in range(len(list_of_gfid) - 1):
                if list_of_gfid[x_count] != list_of_gfid[x_count + 1]:
                    flag = False
            self.assertTrue(flag, ("The gfid for the directory %s is not "
                                   "same on all the bricks", direc))
        g.log.info("The gfid for each directory is the same on all the "
                   "bricks")

        for direc in list_of_all_dirs:
            list_of_xattrs = get_fattr_list(self.mounts[0].client_system,
                                            self.mounts[0].mountpoint
                                            + '/' + direc)
            if 'security.selinux' in list_of_xattrs:
                del list_of_xattrs['security.selinux']
            self.assertFalse(list_of_xattrs, "one or more xattr being "
                                             "displayed on mount point")
        g.log.info("Verified : mount point not displaying important "
                   "xattrs")

        for direc in list_of_all_dirs:
            fattr = get_fattr(self.mounts[0].client_system,
                              self.mounts[0].mountpoint+'/'+direc,
                              'trusted.glusterfs.pathinfo')
            self.assertTrue(fattr, ("Pathinfo not displayed for the "
                                    "directory %s on mount point", direc))
        brick_list = get_all_bricks(self.mnode, self.volname)
        for direc in list_of_all_dirs:
            for brick in brick_list:
                host = brick.partition(':')[0]
                brick_path = brick.partition(':')[2]
                fattr = get_fattr(host, brick_path + '/' + direc,
                                  'trusted.glusterfs.pathinfo')
                self.assertIsNone(fattr, "subvolume displaying pathinfo")
        g.log.info("Verified: only mount point showing pathinfo "
                   "for all the directories")

    def test_create_link_for_directory(self):

        m_point = self.mounts[0].mountpoint
        fqpath_for_test_dir = m_point + '/test_dir'
        flag = mkdir(self.clients[0], fqpath_for_test_dir, True)
        self.assertTrue(flag, "Failed to create a directory")
        fqpath = m_point + '/test_dir/dir{1..3}'
        flag = mkdir(self.clients[0], fqpath, True)
        self.assertTrue(flag, "Failed to create sub directories")
        flag = validate_files_in_dir(self.clients[0],
                                     fqpath_for_test_dir,
                                     test_type=const.TEST_LAYOUT_IS_COMPLETE)
        self.assertTrue(flag, "Layout of test directory is not complete")
        g.log.info("Layout for directory is complete")

        sym_link_path = m_point + '/' + 'test_sym_link'
        command = 'ln -s ' + fqpath_for_test_dir + ' ' + sym_link_path
        ret, _, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Failed to create symlink for test_dir")

        command = 'stat ' + sym_link_path
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Stat command didn't return the details "
                                 "correctly")
        flag = False
        if 'symbolic link' in out:
            flag = True
        self.assertTrue(flag, "The type of the link is not symbolic")
        g.log.info("The link is symbolic")
        flag = False
        if search(fqpath_for_test_dir, out):
            flag = True
        self.assertTrue(flag, "sym link does not point to correct "
                              "location")
        g.log.info("sym link points to right directory")
        g.log.info("The details of the symlink are correct")

        command = 'ls -id ' + fqpath_for_test_dir + ' ' + sym_link_path
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Inode numbers not retrieved by the "
                                 "ls command")
        list_of_inode_numbers = out.split('\n')
        if (list_of_inode_numbers[0].split(' ')[0] ==
                list_of_inode_numbers[1].split(' ')[0]):
            flag = False
        self.assertTrue(flag, "The inode numbers of the dir and sym link "
                              "are same")
        g.log.info("Verified: inode numbers of the test_dir "
                   "and its sym link are different")

        command = 'ls ' + sym_link_path
        ret, out1, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Failed to list the contents using the "
                                 "sym link")
        command = 'ls ' + fqpath_for_test_dir
        ret, out2, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "Failed to list the contents of the "
                                 "test_dir using ls command")
        flag = False
        if out1 == out2:
            flag = True
        self.assertTrue(flag, "The contents listed using the sym link "
                              "are not the same")
        g.log.info("The contents listed using the symlink are"
                   " the same as that of the test_dir")

        command = 'getfattr -d -m . -e hex ' + sym_link_path
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "failed to retrieve xattrs")
        list_xattrs = ['trusted.gfid', 'trusted.glusterfs.dht']
        for xattr in list_xattrs:
            if xattr in out:
                flag = False
        self.assertTrue(flag, "Important xattrs are being compromised"
                              " using the symlink at the mount point")
        g.log.info("Verified: mount point doesn't display important "
                   "xattrs using the symlink")

        path_info_1 = get_pathinfo(self.mounts[0].client_system,
                                   fqpath_for_test_dir)
        path_info_2 = get_pathinfo(self.mounts[0].client_system,
                                   sym_link_path)
        if path_info_1 == path_info_2:
            flag = True
        self.assertTrue(flag, "Pathinfos for test_dir and its sym link "
                              "are not same")
        g.log.info("Pathinfos for test_dir and its sym link are same")

        command = 'readlink ' + sym_link_path
        ret, out, _ = g.run(self.mounts[0].client_system, command)
        self.assertEqual(ret, 0, "readlink command returned an error")
        flag = False
        if out.rstrip() == fqpath_for_test_dir:
            flag = True
        self.assertTrue(flag, "readlink did not return the path of the "
                              "test_dir")
        g.log.info("readlink successfully returned the path of the test_dir")

    def test_copy_directory(self):

        g.set_log_level('glustolog', 'glustolog2', 'WARNING')
        # Checking when destination directory for copying directory doesn't
        # exist
        self.destination_exists = False
        self.copy_dir()

        # Checking by creating destination directory first and then copying
        # created directory
        self.destination_exists = True
        self.copy_dir()

    def test_create_file(self):
        '''
        Test file creation.
        '''
        # pylint: disable=too-many-locals
        # pylint: disable=protected-access
        # pylint: disable=too-many-statements
        mount_obj = self.mounts[0]
        mountpoint = mount_obj.mountpoint

        # files that needs to be created
        file_one = mountpoint + '/file1'

        # hash for file_one
        filehash = calculate_hash(self.servers[0], 'file1')

        # collect subvol info
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']
        secondary_bricks = []
        for subvol in subvols:
            secondary_bricks.append(subvol[0])

        brickobject = []
        for item in secondary_bricks:
            temp = BrickDir(item)
            brickobject.append(temp)

        # create a file
        ret, _, _ = g.run(self.clients[0], ("touch %s" % file_one))
        self.assertEqual(ret, 0, ("File %s creation failed", file_one))

        # get pathinfo xattr on the file
        ret, out, err = g.run(self.clients[0],
                              ("getfattr -n trusted.glusterfs.pathinfo %s" %
                               file_one))
        g.log.info("pathinfo o/p %s", out)
        self.assertEqual(ret, 0, ("failed to get pathinfo on file %s err %s",
                                  file_one, err))

        vol_type = self.volume_type
        if vol_type == "distributed":
            brickhost = (out.split(":"))[3]
            brickpath = (out.split(":"))[4].split(">")[0]
        else:
            brickhost = (out.split(":"))[4]
            brickpath = (out.split(":")[5]).split(">")[0]

        g.log.debug("brickhost %s brickpath %s", brickhost, brickpath)

        # make sure the file is present only on the hashed brick
        count = -1
        for brickdir in brickobject:
            count += 1
            ret = brickdir.hashrange_contains_hash(filehash)
            if ret:
                hash_subvol = subvols[count]
                ret, _, err = g.run(brickdir._host, ("stat %s/file1" %
                                                     brickdir._fqpath))
                g.log.info("Hashed subvol is %s", brickdir._host)
                self.assertEqual(ret, 0, "Expected stat to succeed for file1")
                continue

            ret, _, err = g.run(brickdir._host, ("stat %s/file1" %
                                                 brickdir._fqpath))
            self.assertEqual(ret, 1, "Expected stat to fail for file1")

        # checking if pathinfo xattr has the right value
        ret, _, _ = g.run(brickhost, ("stat %s" % brickpath))
        self.assertEqual(ret, 0, ("Expected file1 to be present on %s",
                                  brickhost))

        # get permission from mount
        ret, out, _ = g.run(self.clients[0], ("ls -l %s" % file_one))
        mperm = (out.split(" "))[0]
        self.assertIsNotNone(mperm, "Expected stat to fail for file1")
        g.log.info("permission on mount %s", mperm)

        # get permission from brick
        ret, out, _ = g.run(brickhost, ("ls -l %s" % brickpath))
        bperm = (out.split(" "))[0]
        self.assertIsNotNone(bperm, "Expected stat to fail for file1")
        g.log.info("permission on brick %s", bperm)

        # check if the permission matches
        self.assertEqual(mperm, bperm, "Expected permission to match")

        # check that gfid xattr is present on the brick
        ret, _, _ = g.run(brickhost, ("getfattr -n trusted.gfid %s" %
                                      brickpath))
        self.assertEqual(ret, 0, "gfid is not present on file")

        # delete the file, bring down it's hash, create the file,
        ret, _, _ = g.run(self.clients[0], ("rm -f %s" % file_one))
        self.assertEqual(ret, 0, "file deletion for file1 failed")
        
        ret = bring_bricks_offline(self.volname, hash_subvol)
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              hash_subvol))

        # check file creation should fail
        ret, _, _ = g.run(self.clients[0], ("touch %s" % file_one))
        self.assertTrue(ret, "Expected file creation to fail")
        
         # Bring up the subvol - restart volume
        ret = volume_start(self.mnode, self.volname, force=True)
        self.assertTrue(ret, "Error in force start the volume")
        g.log.info('Volume restart success')
        sleep(10)

    def _create_file_using_touch(self, file_name):
        """Creates a regular empty file"""
        cmd = "touch {}/{}".format(self.m_point, file_name)
        ret, _, _ = g.run(self.client, cmd)
        self.assertEqual(ret, 0, "Failed to create file {}".format(file_name))
        g.log.info("Successfully created file %s", file_name)

    def _check_file_stat_on_mountpoint(self, file_name, file_type):
        """Check the file-type on mountpoint"""
        file_stat = (get_file_stat(self.client, "{}/{}".format(
            self.m_point, file_name
        )))['filetype']
        self.assertEqual(file_stat, file_type,
                         "File is not a {}".format(file_type))
        g.log.info("File is %s", file_type)

    def _is_file_present_on_brick(self, file_name):
        """Check if file is created on the backend-bricks as per
        the value of trusted.glusterfs.pathinfo xattr"""
        brick_list = get_pathinfo(self.client, "{}/{}".format(
            self.m_point, file_name))
        self.assertNotEqual(
            brick_list, 0, "Failed to get bricklist for {}".format(file_name))

        for brick in brick_list['brickdir_paths']:
            host, path = brick.split(':')
            ret = file_exists(host, path)
            self.assertTrue(ret, "File {} is not present on {}".format(
                file_name, brick
            ))
            g.log.info("File %s is present on %s", file_name, brick)

    def _compare_file_permissions(self, file_name,
                                  file_info_mnt=None, file_info_brick=None):
        """Check if the file's permission are same on mountpoint and
        backend-bricks"""
        if (file_info_mnt is None and file_info_brick is None):
            file_info_mnt = (get_file_stat(self.client, "{}/{}".format(
                self.m_point, file_name
                )))['access']
            self.assertIsNotNone(
                file_info_mnt, "Failed to get access time for {}".format(
                    file_name))
            brick_list = get_pathinfo(self.client, "{}/{}".format(
                self.m_point, file_name))
            self.assertNotEqual(
                brick_list, 0, "Failed to get bricklist for {}".format(
                    file_name))
            file_info_brick = []
            for brick in brick_list['brickdir_paths']:
                host, path = brick.split(':')
                info_brick = (get_file_stat(host, path))['access']
                file_info_brick.append(info_brick)

        for info in file_info_brick:
            self.assertEqual(info, file_info_mnt,
                             "File details for {} are diffrent on"
                             " backend-brick".format(file_name))
            g.log.info("Details for file %s is correct"
                       " on backend-bricks", file_name)

    def _check_change_time_mnt(self, file_name):
        """Find out the modification time for file on mountpoint"""
        file_ctime_mnt = (get_file_stat(self.client, "{}/{}".format(
            self.m_point, file_name
        )))['epoch_ctime']
        return file_ctime_mnt

    def _check_change_time_brick(self, file_name):
        """Find out the modification time for file on backend-bricks"""
        brick_list = get_pathinfo(self.client, "{}/{}".format(
            self.m_point, file_name))
        self.assertNotEqual(brick_list, 0,
                            "Failed to get bricklist for {}".format(file_name))

        brick_mtime = []
        for brick in brick_list['brickdir_paths']:
            host, path = brick.split(':')
            cmd = "ls -lR {}".format(path)
            ret, _, _ = g.run(host, cmd)
            self.assertEqual(ret, 0, "Lookup failed on"
                             " brick:{}".format(path))
            file_ctime_brick = (get_file_stat(host, path))['epoch_ctime']
            brick_mtime.append(file_ctime_brick)
        return brick_mtime

    def _compare_file_perm_mnt(self, mtime_before, mtime_after,
                               file_name):
        """Compare the file permissions before and after appending data"""
        self.assertNotEqual(mtime_before, mtime_after, "Unexpected:"
                            "The ctime has not been changed")
        g.log.info("The modification time for %s has been"
                   " changed as expected", file_name)

    def _collect_and_compare_file_info_on_mnt(
            self, link_file_name, values, expected=True):
        """Collect the files's permissions on mountpoint and compare"""
        stat_test_file = get_file_stat(
            self.client, "{}/test_file".format(self.m_point))
        self.assertIsNotNone(stat_test_file, "Failed to get stat of test_file")
        stat_link_file = get_file_stat(
            self.client, "{}/{}".format(self.m_point, link_file_name))
        self.assertIsNotNone(stat_link_file, "Failed to get stat of {}".format(
            link_file_name))

        for key in values:
            if expected is True:
                self.assertEqual(stat_test_file[key], stat_link_file[key],
                                 "The {} is not same for test_file"
                                 " and {}".format(key, link_file_name))
                g.log.info("The %s for test_file and %s is same on mountpoint",
                           key, link_file_name)
            else:
                self.assertNotEqual(stat_test_file[key], stat_link_file[key],
                                    "Unexpected : The {} is same for test_file"
                                    " and {}".format(key, link_file_name))
                g.log.info("The %s for test_file and %s is different"
                           " on mountpoint", key, link_file_name)

    def _compare_file_md5sum_on_mnt(self, link_file_name):
        """Collect and compare the md5sum for file on mountpoint"""
        md5sum_test_file, _ = (get_md5sum(
            self.client, "{}/test_file".format(self.m_point))).split()
        self.assertIsNotNone(
            md5sum_test_file, "Failed to get md5sum for test_file")

        md5sum_link_file, _ = get_md5sum(
            self.client, "{}/{}".format(self.m_point, link_file_name)).split()
        self.assertIsNotNone(md5sum_link_file, "Failed to get"
                             " md5sum for {}".format(link_file_name))
        self.assertEqual(md5sum_test_file, md5sum_link_file,
                         "The md5sum for test_file and {} is"
                         " not same".format(link_file_name))
        g.log.info("The md5sum is same for test_file and %s"
                   " on mountpoint", link_file_name)

    def _compare_file_md5sum_on_bricks(self, link_file_name):
        """Collect and compare md5sum for file on backend-bricks"""
        brick_list_test_file = get_pathinfo(self.client, "{}/test_file".format(
            self.m_point))
        md5sum_list_test_file = []
        for brick in brick_list_test_file['brickdir_paths']:
            host, path = brick.split(':')
            md5sum_test_file, _ = (get_md5sum(host, path)).split()
            md5sum_list_test_file.append(md5sum_test_file)

        brick_list_link_file = get_pathinfo(self.client, "{}/{}".format(
            self.m_point, link_file_name))
        md5sum_list_link_file = []
        for brick in brick_list_link_file['brickdir_paths']:
            md5sum_link_file, _ = (get_md5sum(host, path)).split()
            md5sum_list_link_file.append(md5sum_link_file)

        self.assertEqual(md5sum_test_file, md5sum_link_file,
                         "The md5sum for test_file and {} is"
                         " not same on brick {}".format(link_file_name, brick))
        g.log.info("The md5sum for test_file and %s is same"
                   " on backend brick %s", link_file_name, brick)

    def _compare_gfid_xattr_on_files(self, link_file_name, expected=True):
        """Collect and compare the value of trusted.gfid xattr for file
        on backend-bricks"""
        brick_list_test_file = get_pathinfo(self.client, "{}/test_file".format(
            self.m_point))
        xattr_list_test_file = []
        for brick in brick_list_test_file['brickdir_paths']:
            host, path = brick.split(':')
            xattr_test_file = get_fattr(host, path, "trusted.gfid")
            xattr_list_test_file.append(xattr_test_file)

        brick_list_link_file = get_pathinfo(self.client, "{}/{}".format(
            self.m_point, link_file_name))
        xattr_list_link_file = []
        for brick in brick_list_link_file['brickdir_paths']:
            host, path = brick.split(':')
            xattr_link_file = get_fattr(host, path, "trusted.gfid")
            xattr_list_link_file.append(xattr_link_file)

        if expected is True:
            self.assertEqual(xattr_list_test_file, xattr_list_link_file,
                             "Unexpected: The xattr trusted.gfid is not same "
                             "for test_file and {}".format(link_file_name))
            g.log.info("The xattr trusted.gfid is same for test_file"
                       " and %s", link_file_name)
        else:
            self.assertNotEqual(xattr_list_test_file, xattr_list_link_file,
                                "Unexpected: The xattr trusted.gfid is same "
                                "for test_file and {}".format(link_file_name))
            g.log.info("The xattr trusted.gfid is not same for test_file"
                       " and %s", link_file_name)

    def test_special_file_creation(self):
        """
        Description : check creation of different types of files.
        Steps:
        1) From mount point, Create a regular file
        eg:
        touch f1
        - From mount point, create character, block device and pipe files
        mknod c
        mknod b
        mkfifo
        2) Stat on the files created in Step-2 from mount point
        3) Verify that file is stored on only one bricks which is mentioned in
           trusted.glusterfs.pathinfo xattr
           On mount point -
           " getfattr -n trusted.glusterfs.pathinfo
           On all bricks
           " ls / "
        4) Verify that file permissions are same on mount point and sub-volumes
           " stat "
        5) Append some data to the file.
        6) List content of file to verify that data has been appended.
           " cat "
        7) Verify that file change time and size has been updated
           accordingly(from mount point and sub-volume)
           " stat / "
        """

        self.client, self.m_point = (self.mounts[0].client_system, self.mounts[0].mountpoint)
        # pylint: disable=too-many-statements
        # pylint: disable=too-many-locals
        # Create a regular file
        self._create_file_using_touch("regfile")

        # Create a character and block file
        for (file_name, parameter) in [
                ("blockfile", "b"), ("charfile", "c")]:
            cmd = "mknod {}/{} {} 1 5".format(self.m_point, file_name,
                                              parameter)
            ret, _, _ = g.run(self.client, cmd)
            self.assertEqual(
                ret, 0, "Failed to create {} file".format(file_name))
            g.log.info("%s file created successfully", file_name)

        # Create a pipe file
        cmd = "mkfifo {}/pipefile".format(self.m_point)
        ret, _, _ = g.run(self.client, cmd)
        self.assertEqual(ret, 0, "Failed to create pipe file")
        g.log.info("Pipe file is created successfully")

        # Stat all the files created on mount-point
        for (file_name, check_string) in [
                ("regfile", "regular empty file"),
                ("charfile", "character special file"),
                ("blockfile", "block special file"),
                ("pipefile", "fifo")]:
            self._check_file_stat_on_mountpoint(file_name, check_string)

        # Verify files are stored on backend bricks as per
        # the trusted.glusterfs.pathinfo
        file_types = ["regfile", "charfile", "blockfile", "pipefile"]

        for file_name in file_types:
            self._is_file_present_on_brick(file_name)

        # Verify that the file permissions are same on
        # mount-point and bricks
        for file_name in file_types:
            self._compare_file_permissions(file_name)

        # Note the modification time on mount and bricks
        # for all files. Also it should be same on mnt and bricks
        reg_mnt_ctime_1 = self._check_change_time_mnt("regfile")
        char_mnt_ctime_1 = self._check_change_time_mnt("charfile")
        block_mnt_ctime_1 = self._check_change_time_mnt("blockfile")
        fifo_mnt_ctime_1 = self._check_change_time_mnt("pipefile")

        reg_brick_ctime_1 = self._check_change_time_brick("regfile")
        char_brick_ctime_1 = self._check_change_time_brick("charfile")
        block_brick_ctime_1 = self._check_change_time_brick("blockfile")
        fifo_brick_ctime_1 = self._check_change_time_brick("pipefile")

        for (file_name, mnt_ctime, brick_ctime) in [
                ("regfile", reg_mnt_ctime_1, reg_brick_ctime_1),
                ("charfile", char_mnt_ctime_1, char_brick_ctime_1),
                ("blockfile", block_mnt_ctime_1, block_brick_ctime_1),
                ("pipefile", fifo_mnt_ctime_1, fifo_brick_ctime_1)]:
            self._compare_file_permissions(
                file_name, mnt_ctime, brick_ctime)

        # Append some data to the files
        for (file_name, data_str) in [
                ("regfile", "regular"),
                ("charfile", "character special"),
                ("blockfile", "block special")]:
            ret = append_string_to_file(
                self.client, "{}/{}".format(self.m_point, file_name),
                "Welcome! This is a {} file".format(data_str))
            self.assertTrue(
                ret, "Failed to append data to {}".format(file_name))
            g.log.info(
                "Successfully appended data to %s", file_name)

        # Check if the data has been appended
        check = "Welcome! This is a regular file"
        cmd = "cat {}/{}".format(self.m_point, "regfile")
        ret, out, _ = g.run(self.client, cmd)
        self.assertEqual(out.strip(), check, "No data present at regfile")

        # Append data to pipefile and check if it has been appended
        g.run_async(self.client, "echo 'Hello' > {}/{} ".format(
            self.m_point, "pipefile"))
        ret, out, _ = g.run(
            self.client, "cat < {}/{}".format(self.m_point, "pipefile"))
        self.assertEqual(
            ret, 0, "Unable to fetch datat on other terimnal")
        self.assertEqual(
            "Hello", out.split('\n')[0],
            "Hello not recieved on the second terimnal")

        # Lookup on mount-point
        cmd = "ls -lR {}".format(self.m_point)
        ret, _, _ = g.run(self.client, cmd)
        self.assertEqual(ret, 0, "Lookup on mountpoint failed")

        # Collect ctime on mount point after appending data
        reg_mnt_ctime_2 = self._check_change_time_mnt("regfile")

        # After appending data the ctime for file should change
        self.assertNotEqual(reg_mnt_ctime_1, reg_mnt_ctime_2, "Unexpected:"
                            "The ctime has not been changed")
        g.log.info("The modification time for regfile has been"
                   " changed as expected")

        # Collect the ctime on bricks
        reg_brick_ctime_2 = self._check_change_time_brick("regfile")

        # Check if the ctime has changed on bricks as per mount
        self._compare_file_permissions(
            "regfile", reg_mnt_ctime_2, reg_brick_ctime_2)
   
    def test_hard_link_file(self):
        """
        Description: link file create, validate and access file
                     using it

        Steps:
        1) From mount point, create a regular file
        2) Verify that file is stored on only on bricks which is
           mentioned in trusted.glusterfs.pathinfo xattr
        3) From mount point create hard-link file for the created file
        4) From mount point stat on the hard-link file and original file;
           file inode, permission, size should be same
        5) From mount point, verify that file contents are same
           "md5sum"
        6) Verify "trusted.gfid" extended attribute of the file
           on sub-vol
        7) From sub-volume stat on the hard-link file and original file;
           file inode, permission, size should be same
        8) From sub-volume verify that content of file are same
        """

        self.client, self.m_point = (self.mounts[0].client_system, self.mounts[0].mountpoint)

        # Create a regular file
        self._create_file_using_touch("test_file")

        # Check file is create on bricks as per trusted.glusterfs.pathinfo
        self._is_file_present_on_brick("test_file")

        # Create a hard-link file for the test_file
        ret = create_link_file(
            self.client, "{}/test_file".format(self.m_point),
            "{}/hardlink_file".format(self.m_point))
        self.assertTrue(ret, "Failed to create hard link file for"
                             " test_file")
        g.log.info("Successfully created hardlink_file")

        # On mountpoint perform stat on original and hard-link file
        values = ["inode", "access", "size"]
        self._collect_and_compare_file_info_on_mnt(
            "hardlink_file", values, expected=True)

        # Check the md5sum on original and hard-link file on mountpoint
        self._compare_file_md5sum_on_mnt("hardlink_file")

        # Compare the value of trusted.gfid for test_file and hard-link file
        # on backend-bricks
        self._compare_gfid_xattr_on_files("hardlink_file")

        # On backend bricks perform stat on original and hard-link file
        values = ["inode", "access", "size"]
        self._collect_and_compare_file_info_on_mnt("hardlink_file", values)

        # On backend bricks check the md5sum
        self._compare_file_md5sum_on_bricks("hardlink_file")

    def test_symlink_file(self):
        """
        Description: Create symbolic link file, validate and access file
                     using it

        Steps:
        1) From mount point, create a regular file
        2) Verify that file is stored on only on bricks which is
           mentioned in trusted.glusterfs.pathinfo xattr
        3) From mount point create symbolic link file for the created file
        4) From mount point stat on the symbolic link file and original file;
           file inode should be different
        5) From mount point, verify that file contents are same
           "md5sum"
        6) Verify "trusted.gfid" extended attribute of the file
           on sub-vol
        7) Verify readlink on symbolic link from mount point
           "readlink "
        8) From sub-volume verify that content of file are same
        """

        self.client, self.m_point = (self.mounts[0].client_system, self.mounts[0].mountpoint)
        # Create a regular file on mountpoint
        self._create_file_using_touch("test_file")

        # Check file is create on bricks as per trusted.glusterfs.pathinfo
        self._is_file_present_on_brick("test_file")

        # Create a symbolic-link file for the test_file
        ret = create_link_file(
            self.client, "{}/test_file".format(self.m_point),
            "{}/softlink_file".format(self.m_point), soft=True)
        self.assertTrue(ret, "Failed to create symbolic link file for"
                             " test_file")
        g.log.info("Successfully created softlink_file")

        # On mountpoint perform stat on original and symbolic-link file
        # The value of inode should be different
        values = ["inode"]
        self._collect_and_compare_file_info_on_mnt(
            "softlink_file", values, expected=False)

        # Check the md5sum on original and symbolic-link file on mountpoint
        self._compare_file_md5sum_on_mnt("softlink_file")

        # Compare the value of trusted.gfid for test_file and
        # symbolic-link file on backend-bricks
        self._compare_gfid_xattr_on_files("softlink_file")

        # Verify readlink on symbolic-link from mount point
        cmd = "readlink {}/softlink_file".format(self.m_point)
        ret, out, _ = g.run(self.client, cmd)
        self.assertEqual(
            out.strip(), "{}/test_file".format(self.m_point),
            "Symbolic link points to incorrect file")
        g.log.info("Symbolic link points to correct file")

        # Check the md5sum on original and symbolic-link file on backend bricks
        self._compare_file_md5sum_on_bricks("softlink_file")

    def test_mkdir_with_subvol_down(self):
        '''
        Test mkdir hashed to a down subvol
        '''
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-branches
        # pylint: disable=too-many-statements
        # pylint: disable=W0212
        mount_obj = self.mounts[0]
        mountpoint = mount_obj.mountpoint

        # directory that needs to be created
        parent_dir = mountpoint + '/parent'
        child_dir = mountpoint + '/parent/child'

        # get hashed subvol for name "parent"
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']
        hashed, count = find_hashed_subvol(subvols, "/", "parent")
        self.assertIsNotNone(hashed, "Could not find hashed subvol")

        # bring target_brick offline
        bring_bricks_offline(self.volname, subvols[count])
        ret = are_bricks_offline(self.mnode, self.volname, subvols[count])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              subvols[count]))
        g.log.info('target subvol is offline')

        # create parent dir
        ret, _, err = g.run(self.clients[0], ("mkdir %s" % parent_dir))
        self.assertNotEqual(ret, 0, ('Expected mkdir of %s to fail with %s',
                                     parent_dir, err))
        g.log.info('mkdir of dir %s failed as expected', parent_dir)

        # check that parent_dir does not exist on any bricks and client
        brickobject = create_brickobjectlist(subvols, "/")
        for brickdir in brickobject:
            adp = "%s/parent" % brickdir.path
            bpath = adp.split(":")
            self.assertTrue((file_exists(brickdir._host, bpath[1])) == 0,
                            ('Expected dir %s not to exist on servers',
                             parent_dir))

        for client in self.clients:
            self.assertTrue((file_exists(client, parent_dir)) == 0,
                            ('Expected dir %s not to exist on clients',
                             parent_dir))

        g.log.info('dir %s does not exist on mount as expected', parent_dir)

        # Bring up the subvols and create parent directory
        bring_bricks_online(self.mnode, self.volname, subvols[count],
                            bring_bricks_online_methods=None)
        ret = are_bricks_online(self.mnode, self.volname, subvols[count])
        self.assertTrue(ret, ("Error in bringing back subvol %s online",
                              subvols[count]))
        g.log.info('Subvol is back online')

        ret, _, _ = g.run(self.clients[0], ("mkdir %s" % parent_dir))
        self.assertEqual(ret, 0, ('Expected mkdir of %s to succeed',
                                  parent_dir))
        g.log.info('mkdir of dir %s successful', parent_dir)

        # get hash subvol for name "child"
        hashed, count = find_hashed_subvol(subvols, "parent", "child")
        self.assertIsNotNone(hashed, "Could not find hashed subvol")

        # bring target_brick offline
        bring_bricks_offline(self.volname, subvols[count])
        ret = are_bricks_offline(self.mnode, self.volname, subvols[count])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              subvols[count]))
        g.log.info('target subvol is offline')

        # create child dir
        ret, _, err = g.run(self.clients[0], ("mkdir %s" % child_dir))
        self.assertNotEqual(ret, 0, ('Expected mkdir of %s to fail with %s',
                                     child_dir, err))
        g.log.info('mkdir of dir %s failed', child_dir)

        # check if child_dir exists on any bricks
        for brickdir in brickobject:
            adp = "%s/parent/child" % brickdir.path
            bpath = adp.split(":")
            self.assertTrue((file_exists(brickdir._host, bpath[1])) == 0,
                            ('Expected dir %s not to exist on servers',
                             child_dir))
        for client in self.clients:
            self.assertTrue((file_exists(client, child_dir)) == 0)

        g.log.info('dir %s does not exist on mount as expected', child_dir)

        # Bring up the subvol - restart volume                                 
        ret = volume_start(self.mnode, self.volname, force=True)                
        self.assertTrue(ret, "Error in force start the volume")                 
        g.log.info('Volume restart success')                                    
        sleep(10)

    def mkdir_post_hashdown(self, subvols, parent_dir):
        '''
        case -1:
        - bring down a subvol
        - create a directory so that it does not hash to down subvol
        - make sure stat is successful on the dir
        '''
        # pylint: disable=protected-access
        # pylint: disable=pointless-string-statement
        # Find a non hashed subvolume(or brick)
        nonhashed_subvol, count = find_nonhashed_subvol(subvols, "/", "parent")
        if nonhashed_subvol is None:
            g.log.error('Error in finding nonhashed subvol for parent')
            return False

        # bring nonhashed_subbvol offline
        ret = bring_bricks_offline(self.volname, subvols[count])
        if ret == 0:
            g.log.error('Error in bringing down subvolume %s',
                        subvols[count])
            return False

        g.log.info('target subvol %s is offline', subvols[count])

        # create parent dir
        ret, _, err = g.run(self.clients[0], ("mkdir %s" % parent_dir))
        if ret != 0:
            g.log.error('mkdir failed for %s err: %s', parent_dir, err)
            return False
        g.log.info("mkdir of parent directory %s successful", parent_dir)

        # this confirms both layout and stat of the directory
        ret = validate_files_in_dir(self.clients[0],
                                    self.mounts[0].mountpoint + '/parent_dir',
                                    test_type=LAYOUT_IS_COMPLETE,
                                    file_type=FILETYPE_DIRS)
        self.assertTrue(ret, "Layout is not complete")
        g.log.info('Layout is complete')

        # bring up the subvol
        ret = bring_bricks_online(self.mnode, self.volname, subvols[count],
                                  bring_bricks_online_methods=None)
        if ret == 0:
            g.log.error("Error in bringing back subvol online")
            return False

        g.log.info('Subvol is back online')

        # delete parent_dir
        ret, _, err = g.run(self.clients[0], ("rmdir %s" % parent_dir))
        if ret != 0:
            g.log.error('rmdir failed for %s err: %s', parent_dir, err)
        g.log.info("rmdir of directory %s successful", parent_dir)

        return True

    def mkdir_before_hashdown(self, subvols, parent_dir):
        '''
        case -2:
            - create directory
            - bring down hashed subvol
            - make sure stat is successful on the dir
        '''
        # pylint: disable=protected-access
        # pylint: disable=pointless-string-statement
        # create parent dir
        ret, _, err = g.run(self.clients[0], ("mkdir %s" % parent_dir))
        if ret != 0:
            g.log.error('mkdir failed for %s err: %s', parent_dir, err)
            return False
        g.log.info("mkdir of parent directory %s successful", parent_dir)

        # find hashed subvol
        hashed_subvol, count = find_hashed_subvol(subvols, "/", "parent")
        if hashed_subvol is None:
            g.log.error('Error in finding hash value')
            return False

        g.log.info("hashed subvol %s", hashed_subvol._host)

        # bring hashed_subvol offline
        ret = bring_bricks_offline(self.volname, subvols[count])
        if ret == 0:
            g.log.error('Error in bringing down subvolume %s', subvols[count])
            return False
        g.log.info('target subvol %s is offline', subvols[count])

        # this confirms both layout and stat of the directory
        ret = validate_files_in_dir(self.clients[0],
                                    self.mounts[0].mountpoint + '/parent_dir',
                                    test_type=LAYOUT_IS_COMPLETE,
                                    file_type=FILETYPE_DIRS)
        self.assertTrue(ret, "Layout is not complete")
        g.log.info('Layout is complete')

        # bring up the subvol
        ret = bring_bricks_online(self.mnode, self.volname, subvols[count],
                                  bring_bricks_online_methods=None)
        if ret == 0:
            g.log.error("Error in bringing back subvol online")
            return False
        g.log.info('Subvol is back online')

        # delete parent_dir
        ret, _, err = g.run(self.clients[0], ("rmdir %s" % parent_dir))
        if ret == 0:
            g.log.error('rmdir failed for %s err: %s', parent_dir, err)
        g.log.info("rmdir of directory %s successful", parent_dir)
        return True

    def mkdir_nonhashed_down(self, subvols, parent_dir):
        '''
        case -3:
            - create dir
            - bringdown a non-hashed subvol
            - make sure stat is successful on the dir
        '''
        # pylint: disable=protected-access
        # pylint: disable=pointless-string-statement
        # create parent dir
        ret, _, err = g.run(self.clients[0], ("mkdir %s" % parent_dir))
        if ret != 0:
            g.log.error('mkdir failed for %s err: %s', parent_dir, err)
            return False

        g.log.info("mkdir of parent directory %s successful", parent_dir)

        # Find a non hashed subvolume(or brick)
        nonhashed_subvol, count = find_nonhashed_subvol(subvols, "/", "parent")
        if nonhashed_subvol is None:
            g.log.error('Error in finding hash value')
            return False

        # bring nonhashed_subbvol offline
        ret = bring_bricks_offline(self.volname, subvols[count])
        if ret == 0:
            g.log.error('Error in bringing down subvolume %s', subvols[count])
            return False
        g.log.info('target subvol %s is offline', subvols[count])

        # this confirms both layout and stat of the directory
        ret = validate_files_in_dir(self.clients[0],
                                    self.mounts[0].mountpoint + '/parent_dir',
                                    test_type=LAYOUT_IS_COMPLETE,
                                    file_type=FILETYPE_DIRS)
        self.assertTrue(ret, "Expected - Layout is complete")
        g.log.info('Layout is complete')

        # bring up the subvol
        ret = bring_bricks_online(self.mnode, self.volname, subvols[count],
                                  bring_bricks_online_methods=None)
        if ret == 0:
            g.log.error("Error in bringing back subvol online")
            return False
        g.log.info('Subvol is back online')

        # delete parent_dir
        ret, _, err = g.run(self.clients[0], ("rmdir %s" % parent_dir))
        if ret != 0:
            g.log.error('rmdir failed for %s err: %s', parent_dir, err)
            return False
        g.log.info("rmdir of directory %s successful", parent_dir)
        return True

    def test_lookup_dir(self):
        '''
        Test directory lookup.
        '''
        # pylint: disable=too-many-locals
        mount_obj = self.mounts[0]
        mountpoint = mount_obj.mountpoint

        # directory that needs to be created
        parent_dir = mountpoint + '/parent'

        # calculate hash for name "parent"
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']

        # This populates one brick from one subvolume
        secondary_bricks = []
        for subvol in subvols:
            secondary_bricks.append(subvol[0])

        for subvol in secondary_bricks:
            g.log.debug("secondary bricks %s", subvol)

        brickobject = []
        for item in secondary_bricks:
            temp = BrickDir(item)
            brickobject.append(temp)

        ret = self.mkdir_post_hashdown(subvols, parent_dir)
        self.assertTrue(ret, 'mkdir_post_hashdown failed')

        ret = self.mkdir_before_hashdown(subvols, parent_dir)
        self.assertTrue(ret, 'mkdir_before_hashdown failed')

        ret = self.mkdir_nonhashed_down(subvols, parent_dir)
        self.assertTrue(ret, 'mkdir_nonhashed_down failed')

    def _validate_io(self):
        """Validare I/O threads running on mount point"""
        io_success = []
        for proc in self.proc_list:
            try:
                ret, _, _ = proc.async_communicate()
                if ret:
                    io_success.append(False)
                    break
                io_success.append(True)
            except ValueError:
                io_success.append(True)
        return all(io_success)

    def test_time_taken_for_ls(self):
        """
        Test case:
        1. Create a volume of type distributed-replicated or
           distributed-arbiter or distributed-dispersed and start it.
        2. Mount the volume to clients and create 2000 directories
           and 10 files inside each directory.
        3. Wait for I/O to complete on mount point and perform ls
           (ls should complete within 10 seconds).
        """

        is_io_running = False
        # Creating 2000 directories on the mount point
        ret, _, _ = g.run(self.mounts[0].client_system,
                          "cd %s; for i in {1..2000};do mkdir dir$i;done"
                          % self.mounts[0].mountpoint)
        self.assertFalse(ret, 'Failed to create 2000 dirs on mount point')

        # Create 5000 files inside each directory
        dirs = ('{1..100}', '{101..200}', '{201..300}', '{301..400}',
                '{401..500}', '{501..600}', '{601..700}', '{701..800}',
                '{801..900}', '{901..1000}', '{1001..1100}', '{1101..1200}',
                '{1201..1300}', '{1301..1400}', '{1401..1500}', '{1501..1600}',
                '{1801..1900}', '{1901..2000}')
        self.proc_list, counter = [], 0
        while counter < 18:
            for mount_obj in self.mounts:
                ret = g.run_async(mount_obj.client_system,
                                  "cd %s;for i in %s;do "
                                  "touch dir$i/file{1..10};done"
                                  % (mount_obj.mountpoint, dirs[counter]))
                self.proc_list.append(ret)
                counter += 1
        is_io_running = True

        # Check if I/O is successful or not
        ret = self._validate_io()
        self.assertTrue(ret, "Failed to create Files and dirs on mount point")
        is_io_running = False
        g.log.info("Successfully created files and dirs needed for the test")

        # Run ls on mount point which should get completed within 10 seconds
        ret, _, _ = g.run(self.mounts[0].client_system,
                          "cd %s; timeout 10 ls"
                          % self.mounts[0].mountpoint)
        self.assertFalse(ret, '1s taking more than 10 seconds')
        g.log.info("ls completed in under 10 seconds")


