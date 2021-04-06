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

from time import sleep
from re import search
from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.gluster_base_class import GlusterBaseClass, runs_on
from glustolibs.gluster.brick_libs import get_all_bricks
from glustolibs.gluster.dht_test_utils import validate_files_in_dir
import glustolibs.gluster.constants as k
from glustolibs.gluster.glusterfile import (get_fattr, get_pathinfo,
                                            get_fattr_list, get_file_stat, get_pathinfo,
                                            file_exists, create_link_file,
                                            get_md5sum)
from glustolibs.gluster.glusterdir import mkdir
from glustolibs.gluster.brick_libs import bring_bricks_offline
from glustolibs.gluster.brick_libs import bring_bricks_online
from glustolibs.gluster.volume_libs import get_subvols
from glustolibs.gluster.dht_test_utils import (
    find_new_hashed,
    find_hashed_subvol)
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


@runs_on([['distributed', 'distributed-replicated',
           'dispersed', 'distributed-dispersed', 'replicated',
           'arbiter', 'distributed-arbiter'],
          ['glusterfs']])
class TestDhtMultiCases(GlusterBaseClass):

    """
    Description: tests to check the dht layouts of files and directories,
                 along with their symlinks.
    """
    def setUp(self):

        # Calling GlusterBaseClass setUp
        self.get_super_method(self, 'setUp')()

        # Setup Volume and Mount Volume
        ret = self.setup_volume_and_mount_volume(self.mounts)
        if not ret:
            raise ExecutionError("Failed to Setup_Volume and Mount_Volume")
        g.log.info("Successful in Setup Volume and Mount Volume")

    def tearDown(self):

        # Unmount and cleanup original volume
        ret = self.unmount_volume_and_cleanup_volume(mounts=self.mounts)
        if not ret:
            raise ExecutionError("Failed to umount the vol & cleanup Volume")
        g.log.info("Successful in umounting the volume and Cleanup")

        # Calling GlusterBaseClass tearDown
        self.get_super_method(self, 'tearDown')()

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
                                     test_type=k.TEST_LAYOUT_IS_COMPLETE)
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
                                     test_type=k.TEST_LAYOUT_IS_COMPLETE)
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

        # Checking when destination directory for copying directory doesn't
        # exist
        self.destination_exists = False
        self.copy_dir()

        # Checking by creating destination directory first and then copying
        # created directory
        self.destination_exists = True
        self.copy_dir()

    def test_file_access(self):
        """
        Test file access.
        """
        # pylint: disable=protected-access
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-statements
        mount_obj = self.mounts[0]
        mountpoint = mount_obj.mountpoint

        # get subvol list
        subvols = (get_subvols(self.mnode, self.volname))['volume_subvols']
        self.assertIsNotNone(subvols, "failed to get subvols")

        # create a file
        srcfile = mountpoint + '/testfile'
        ret, _, err = g.run(self.clients[0], ("touch %s" % srcfile))
        self.assertEqual(ret, 0, ("File creation failed for %s err %s",
                                  srcfile, err))
        g.log.info("testfile creation successful")

        # find hashed subvol
        srchashed, scount = find_hashed_subvol(subvols, "/", "testfile")
        self.assertIsNotNone(srchashed, "could not find srchashed")
        g.log.info("hashed subvol for srcfile %s subvol count %s",
                   srchashed._host, str(scount))

        # rename the file such that the new name hashes to a new subvol
        tmp = find_new_hashed(subvols, "/", "testfile")
        self.assertIsNotNone(tmp, "could not find new hashed for dstfile")
        g.log.info("dst file name : %s dst hashed_subvol : %s "
                   "subvol count : %s", tmp.newname,
                   tmp.hashedbrickobject._host, str(tmp.subvol_count))

        dstname = str(tmp.newname)
        dstfile = mountpoint + "/" + dstname
        dsthashed = tmp.hashedbrickobject
        dcount = tmp.subvol_count
        ret, _, err = g.run(self.clients[0], ("mv %s %s" %
                                              (srcfile, dstfile)))
        self.assertEqual(ret, 0, ("rename failed for %s err %s",
                                  srcfile, err))
        g.log.info("cmd: mv srcfile dstfile successful")

        # check that on dsthash_subvol the file is a linkto file
        filepath = dsthashed._fqpath + "/" + dstname
        file_stat = get_file_stat(dsthashed._host, filepath)
        self.assertEqual(file_stat['access'], "1000", ("Expected file "
                                                       "permission to be 1000"
                                                       " on subvol %s",
                                                       dsthashed._host))
        g.log.info("dsthash_subvol has the expected linkto file")

        # check on srchashed the file is a data file
        filepath = srchashed._fqpath + "/" + dstname
        file_stat = get_file_stat(srchashed._host, filepath)
        self.assertNotEqual(file_stat['access'], "1000", ("Expected file "
                                                          "permission not to"
                                                          "be 1000 on subvol"
                                                          "%s",
                                                          srchashed._host))

        # Bring down the hashed subvol of dstfile(linkto file)
        ret = bring_bricks_offline(self.volname, subvols[dcount])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              subvols[dcount]))
        g.log.info('dst subvol %s is offline', subvols[dcount])

        # Need to access the file through a fresh lookup through a new mount
        # create a new dir(choosing server to do a mount)
        ret, _, _ = g.run(self.mnode, ("mkdir -p /mnt"))
        self.assertEqual(ret, 0, ('mkdir of mount dir failed'))
        g.log.info("mkdir of mount dir succeeded")

        # do a temp mount
        ret = mount_volume(self.volname, self.mount_type, "/mnt",
                           self.mnode, self.mnode)
        self.assertTrue(ret, ('temporary mount failed'))
        g.log.info("temporary mount succeeded")

        # check that file is accessible (stat)
        ret, _, _ = g.run(self.mnode, ("stat /mnt/%s" % dstname))
        self.assertEqual(ret, 0, ('stat error on for dst file %s', dstname))
        g.log.info("stat on /mnt/%s successful", dstname)

        # cleanup temporary mount
        ret = umount_volume(self.mnode, "/mnt")
        self.assertTrue(ret, ('temporary mount failed'))
        g.log.info("umount successful")

        # Bring up the hashed subvol
        ret = bring_bricks_online(self.mnode, self.volname, subvols[dcount],
                                  bring_bricks_online_methods=None)
        self.assertTrue(ret, "Error in bringing back subvol online")
        g.log.info('Subvol is back online')

        # now bring down the cached subvol
        ret = bring_bricks_offline(self.volname, subvols[scount])
        self.assertTrue(ret, ('Error in bringing down subvolume %s',
                              subvols[scount]))
        g.log.info('target subvol %s is offline', subvols[scount])

        # file access should fail
        ret, _, _ = g.run(self.clients[0], ("stat %s" % dstfile))
        self.assertEqual(ret, 1, ('stat error on for file %s', dstfile))
        g.log.info("dstfile access failed as expected")

    def test_distribution_hash_value(self):
        """Test case tests DHT of files and directories based on hash value
        """
        # pylint: disable=too-many-locals
        for client_index, mount_obj in enumerate(self.mounts):
            client_host = mount_obj.client_system
            mountpoint = mount_obj.mountpoint

            # Create directory for initial data
            g.log.debug("Creating temporary folder on client's machine %s:%s",
                        client_host, self.temp_folder)
            if not mkdir(client_host, self.temp_folder):
                g.log.error("Failed create temporary directory "
                            "on client machine %s:%s",
                            client_host, self.temp_folder)
                raise ExecutionError("Failed create temporary directory "
                                     "on client machine %s:%s" %
                                     (client_host, self.temp_folder))
            g.log.info('Created temporary directory on client machine %s:%s',
                       client_host, self.temp_folder)
            # Prepare a set of data
            files = ["{prefix}{file_name}_{client_index}".
                     format(file_name=file_name,
                            client_index=client_index,
                            prefix='' if randint(1, 6) % 2
                            else choice('ABCD') + '/')
                     for file_name in map(chr, range(97, 123))]
            ret = self.create_files(client_host, self.temp_folder,
                                    files,
                                    "Lorem Ipsum is simply dummy text of the "
                                    "printing and typesetting industry.")
            self.assertTrue(ret, "Failed creating a set of files and dirs "
                                 "on %s:%s" % (client_host, self.temp_folder))
            g.log.info('Created data set on client machine on folder %s:%s',
                       client_host, self.temp_folder)

            # Copy prepared data to mount point
            cmd = ('cp -vr {source}/* {destination}'.format(
                source=self.temp_folder,
                destination=mountpoint))
            ret, _, _ = g.run(client_host, cmd)
            self.assertEqual(ret, 0, "Copy data to mount point %s:%s Failed")
            g.log.info('Copied prepared data to mount point %s:%s',
                       client_host, mountpoint)

            # Verify that hash layout values are set on each
            # bricks for the dir
            g.log.debug("Verifying DHT layout")
            ret = validate_files_in_dir(client_host, mountpoint,
                                        test_type=TEST_LAYOUT_IS_COMPLETE)
            self.assertTrue(ret, "TEST_LAYOUT_IS_COMPLETE: FAILED")
            g.log.info("TEST_LAYOUT_IS_COMPLETE: PASS on %s:%s ",
                       client_host, mountpoint)

            g.log.debug("Verifying files and directories")
            ret = validate_files_in_dir(client_host, mountpoint,
                                        test_type=FILE_ON_HASHED_BRICKS,
                                        file_type=FILETYPE_DIRS)
            self.assertTrue(ret, "TEST_FILE_EXISTS_ON_HASHED_BRICKS: FAILED")
            g.log.info("TEST_FILE_EXISTS_ON_HASHED_BRICKS: PASS")

            # Verify "trusted.gfid" extended attribute of the
            # directory/file on all the bricks
            gfids = dict()
            g.log.debug("Check if trusted.gfid is presented on the bricks")
            for brick_item in get_all_bricks(self.mnode, self.volname):
                brick_host, brick_dir = brick_item.split(':')

                for target_destination in files:
                    if not file_exists(brick_host, '{brick_dir}/{dest}'.
                                       format(brick_dir=brick_dir,
                                              dest=target_destination)):
                        continue
                    ret = get_fattr(brick_host, '%s/%s' %
                                    (brick_dir, target_destination),
                                    'trusted.gfid')
                    self.assertIsNotNone(ret,
                                         "trusted.gfid is not presented "
                                         "on %s/%s" % (brick_dir,
                                                       target_destination))
                    g.log.info("Verified trusted.gfid on brick %s:%s",
                               brick_item, target_destination)
                    gfids.setdefault(target_destination, []).append(ret)

            g.log.debug('Check if trusted.gfid is same on all the bricks')
            self.assertTrue(all([False if len(set(gfids[k])) > 1 else True
                                 for k in gfids]),
                            "trusted.gfid should be same on all the bricks")
            g.log.info('trusted.gfid is same on all the bricks')
            # Verify that mount point shows pathinfo xattr.
            g.log.debug("Check if pathinfo is presented on mount point "
                        "%s:%s", client_host, mountpoint)
            ret = get_fattr(client_host, mountpoint,
                            'trusted.glusterfs.pathinfo')
            self.assertIsNotNone(ret, "pathinfo is not presented on mount "
                                      "point %s:%s" % (client_host,
                                                       mountpoint))

            g.log.info('trusted.glusterfs.pathinfo is presented on mount'
                       ' point %s:%s', client_host, mountpoint)

            # Mount point should not display xattr:
            # trusted.gfid and trusted.glusterfs.dht
            g.log.debug("Check if trusted.gfid and trusted.glusterfs.dht are "
                        "not presented on mount point %s:%s", client_host,
                        mountpoint)
            attributes = get_fattr_list(client_host, mountpoint)
            self.assertFalse('trusted.gfid' in attributes,
                             "Expected: Mount point shouldn't display xattr:"
                             "{xattr}. Actual: xattrs {xattr} is "
                             "presented on mount point".
                             format(xattr='trusted.gfid'))
            self.assertFalse('trusted.glusterfs.dht' in attributes,
                             "Expected: Mount point shouldn't display xattr:"
                             "{xattr}. Actual: xattrs {xattr} is "
                             "presented on mount point".
                             format(xattr='trusted.glusterfs.dht'))

            g.log.info("trusted.gfid and trusted.glusterfs.dht are not "
                       "presented on mount point %s:%s", client_host,
                       mountpoint)
        g.log.info('Files and dirs are stored on bricks based on hash value')

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
        self.is_io_running = True

        # Check if I/O is successful or not
        ret = self._validate_io()
        self.assertTrue(ret, "Failed to create Files and dirs on mount point")
        self.is_io_running = False
        g.log.info("Successfully created files and dirs needed for the test")

        # Run ls on mount point which should get completed within 10 seconds
        ret, _, _ = g.run(self.mounts[0].client_system,
                          "cd %s; timeout 10 ls"
                          % self.mounts[0].mountpoint)
        self.assertFalse(ret, '1s taking more than 10 seconds')
        g.log.info("ls completed in under 10 seconds")

    def test_rename_directory_no_destination_folder(self):
        """Test rename directory with no destination folder"""
        dirs = {
            'initial': '{root}/folder_{client_index}',
            'new_folder': '{root}/folder_renamed{client_index}'
        }

        for mount_index, mount_obj in enumerate(self.mounts):
            client_host = mount_obj.client_system
            mountpoint = mount_obj.mountpoint
            initial_folder = dirs['initial'].format(
                root=mount_obj.mountpoint,
                client_index=mount_index
            )

            ret = validate_files_in_dir(client_host, mountpoint,
                                        test_type=LAYOUT_IS_COMPLETE,
                                        file_type=FILETYPE_DIRS)
            self.assertTrue(ret, "Expected - Layout is complete")
            g.log.info('Layout is complete')

            # Create source folder on mount point
            self.assertTrue(mkdir(client_host, initial_folder),
                            'Failed creating source directory')
            self.assertTrue(file_exists(client_host, initial_folder))
            g.log.info('Created source directory %s on mount point %s',
                       initial_folder, mountpoint)

            # Create files and directories
            ret = self.create_files(client_host, initial_folder, self.files,
                                    content='Textual content')

            self.assertTrue(ret, 'Unable to create files on mount point')
            g.log.info('Files and directories are created')

            ret = validate_files_in_dir(client_host, mountpoint,
                                        test_type=FILE_ON_HASHED_BRICKS)
            self.assertTrue(ret, "Expected - Files and dirs are stored "
                            "on hashed bricks")
            g.log.info('Files and dirs are stored on hashed bricks')

            new_folder_name = dirs['new_folder'].format(
                root=mountpoint,
                client_index=mount_index
            )
            # Check if destination dir does not exist
            self.assertFalse(file_exists(client_host, new_folder_name),
                             'Expected New folder name should not exists')
            # Rename source folder
            ret = move_file(client_host, initial_folder,
                            new_folder_name)
            self.assertTrue(ret, "Rename direcoty failed")
            g.log.info('Renamed directory %s to %s', initial_folder,
                       new_folder_name)

            # Old dir does not exists and destination is presented
            self.assertFalse(file_exists(client_host, initial_folder),
                             '%s should be not listed' % initial_folder)
            g.log.info('The old directory %s does not exists on mount point',
                       initial_folder)
            self.assertTrue(file_exists(client_host, new_folder_name),
                            'Destination dir does not exists %s' %
                            new_folder_name)
            g.log.info('The new folder is presented %s', new_folder_name)

            # Check bricks for source and destination directories
            for brick_item in get_all_bricks(self.mnode, self.volname):
                brick_host, brick_dir = brick_item.split(':')

                initial_folder = dirs['initial'].format(
                    root=brick_dir,
                    client_index=mount_index
                )
                new_folder_name = dirs['new_folder'].format(
                    root=brick_dir,
                    client_index=mount_index
                )

                self.assertFalse(file_exists(brick_host, initial_folder),
                                 "Expected folder %s to be not presented" %
                                 initial_folder)
                self.assertTrue(file_exists(brick_host, new_folder_name),
                                'Expected folder %s to be presented' %
                                new_folder_name)

                g.log.info('The old directory %s does not exists and directory'
                           ' %s is presented', initial_folder, new_folder_name)
        g.log.info('Rename directory when destination directory '
                   'does not exists is successful')

    def test_rename_directory_with_dest_folder(self):
        """Test rename directory with presented destination folder
        """
        dirs = {
            'initial_folder': '{root}/folder_{client_index}/',
            'new_folder': '{root}/new_folder_{client_index}/'
        }

        for mount_index, mount_obj in enumerate(self.mounts):
            client_host = mount_obj.client_system
            mountpoint = mount_obj.mountpoint

            initial_folder = dirs['initial_folder'].format(
                root=mount_obj.mountpoint,
                client_index=mount_index
            )

            ret = validate_files_in_dir(client_host, mountpoint,
                                        test_type=LAYOUT_IS_COMPLETE,
                                        file_type=FILETYPE_DIRS)
            self.assertTrue(ret, "Expected - Layout is complete")
            g.log.info('Layout is complete')

            # Create a folder on mount point
            self.assertTrue(mkdir(client_host, initial_folder, parents=True),
                            'Failed creating source directory')
            self.assertTrue(file_exists(client_host, initial_folder))
            g.log.info('Created source directory %s on mount point %s',
                       initial_folder, mountpoint)

            new_folder_name = dirs['new_folder'].format(
                root=mountpoint,
                client_index=mount_index
            )
            # Create destination directory
            self.assertTrue(mkdir(client_host, new_folder_name, parents=True),
                            'Failed creating destination directory')
            self.assertTrue(file_exists(client_host, new_folder_name))
            g.log.info('Created destination directory %s on mount point %s',
                       new_folder_name, mountpoint)

            # Create files and directories
            ret = self.create_files(client_host, initial_folder, self.files,
                                    content='Textual content')
            self.assertTrue(ret, 'Unable to create files on mount point')
            g.log.info('Files and directories are created')

            ret = validate_files_in_dir(client_host, mountpoint,
                                        test_type=FILE_ON_HASHED_BRICKS)
            self.assertTrue(ret, "Expected - Files and dirs are stored "
                            "on hashed bricks")
            g.log.info('Files and dirs are stored on hashed bricks')

            # Rename source folder to destination
            ret = move_file(client_host, initial_folder,
                            new_folder_name)
            self.assertTrue(ret, "Rename folder failed")
            g.log.info('Renamed folder %s to %s', initial_folder,
                       new_folder_name)

            # Old dir does not exists and destination is presented
            self.assertFalse(file_exists(client_host, initial_folder),
                             '%s should be not listed' % initial_folder)
            g.log.info('The old directory %s does not exists on mount point',
                       initial_folder)
            self.assertTrue(file_exists(client_host, new_folder_name),
                            'Renamed directory does not exists %s' %
                            new_folder_name)
            g.log.info('The new folder exists %s', new_folder_name)

            # Check bricks for source and destination directories
            for brick_item in get_all_bricks(self.mnode, self.volname):
                brick_host, brick_dir = brick_item.split(':')

                initial_folder = dirs['initial_folder'].format(
                    root=brick_dir,
                    client_index=mount_index
                )
                new_folder_name = dirs['new_folder'].format(
                    root=brick_dir,
                    client_index=mount_index
                )

                self.assertFalse(file_exists(brick_host, initial_folder),
                                 "Expected folder %s to be not presented" %
                                 initial_folder)
                self.assertTrue(file_exists(brick_host, new_folder_name),
                                'Expected folder %s to be presented' %
                                new_folder_name)

                g.log.info('The old directory %s does not exists and directory'
                           ' %s is presented', initial_folder, new_folder_name)
        g.log.info('Rename directory when destination directory '
                   'exists is successful')

    def _create_two_sparse_files(self):
        """Create 2 sparse files from /dev/zero and /dev/null"""

        # Create a tuple to hold both the file names
        self.sparse_file_tuple = (
            "{}/sparse_file_zero".format(self.mounts[0].mountpoint),
            "{}/sparse_file_null".format(self.mounts[0].mountpoint)
            )

        # Create 2 spares file where one is created from /dev/zero and
        # another is created from /dev/null
        for filename, input_file in ((self.sparse_file_tuple[0], "/dev/zero"),
                                     (self.sparse_file_tuple[1], "/dev/null")):
            cmd = ("dd if={} of={} bs=1M seek=5120 count=1000"
                   .format(input_file, filename))
            ret, _, _ = g.run(self.first_client, cmd)
            self.assertEqual(ret, 0, 'Failed to create %s ' % filename)

        g.log.info("Successfully created sparse_file_zero and"
                   " sparse_file_null")

    def _check_du_and_ls_of_sparse_file(self):
        """Check du and ls -lks on spare files"""

        for filename in self.sparse_file_tuple:

            # Fetch output of ls -lks for the sparse file
            cmd = "ls -lks {}".format(filename)
            ret, out, _ = g.run(self.first_client, cmd)
            self.assertEqual(ret, 0, "Failed to get ls -lks for file %s "
                             % filename)
            ls_value = out.split(" ")[5]

            # Fetch output of du for the sparse file
            cmd = "du --block-size=1 {}".format(filename)
            ret, out, _ = g.run(self.first_client, cmd)
            self.assertEqual(ret, 0, "Failed to get du for file %s "
                             % filename)
            du_value = out.split("\t")[0]

            # Compare du and ls -lks value
            self. assertNotEqual(ls_value, du_value,
                                 "Unexpected: Sparse file size coming up same "
                                 "for du and ls -lks")

        g.log.info("Successfully checked sparse file size using ls and du")

    def _delete_two_sparse_files(self):
        """Delete sparse files"""

        for filename in self.sparse_file_tuple:
            cmd = "rm -rf {}".format(filename)
            ret, _, _ = g.run(self.first_client, cmd)
            self.assertEqual(ret, 0, 'Failed to delete %s ' % filename)

        g.log.info("Successfully remove both sparse files")

    def test_sparse_file_creation_and_deletion(self):
        """
        Test case:
        1. Create volume with 5 sub-volumes, start and mount it.
        2. Check df -h for available size.
        3. Create 2 sparse file one from /dev/null and one from /dev/zero.
        4. Find out size of files and compare them through du and ls.
           (They shouldn't match.)
        5. Check df -h for available size.(It should be less than step 2.)
        6. Remove the files using rm -rf.
        """
        # Check df -h for avaliable size
        available_space_at_start = get_size_of_mountpoint(
            self.first_client, self.mounts[0].mountpoint)
        self.assertIsNotNone(available_space_at_start,
                             "Failed to get available space on mount point")

        # Create 2 sparse file one from /dev/null and one from /dev/zero
        self._create_two_sparse_files()

        # Find out size of files and compare them through du and ls
        # (They shouldn't match)
        self._check_du_and_ls_of_sparse_file()

        # Check df -h for avaliable size(It should be less than step 2)
        available_space_now = get_size_of_mountpoint(
            self.first_client, self.mounts[0].mountpoint)
        self.assertIsNotNone(available_space_now,
                             "Failed to get avaliable space on mount point")
        ret = (int(available_space_at_start) > int(available_space_now))
        self.assertTrue(ret, "Available space at start not less than "
                        "available space now")

        # Remove the files using rm -rf
        self._delete_two_sparse_files()

        # Sleep for 180 seconds for the meta data in .glusterfs directory
        # to be removed
        sleep(180)

        # Check df -h after removing sparse files
        available_space_now = get_size_of_mountpoint(
            self.first_client, self.mounts[0].mountpoint)
        self.assertIsNotNone(available_space_now,
                             "Failed to get avaliable space on mount point")
        ret = int(available_space_at_start) - int(available_space_now) < 1500
        self.assertTrue(ret, "Available space at start and available space now"
                        " is not equal")

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
