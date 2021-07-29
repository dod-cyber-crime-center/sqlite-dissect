# Initially generated with the "pyinstaller main.py --onefile" command.  Altered after for minor changes.
# Consecutively run after modifications from the project root directory as:
# pyinstaller pyinstaller\sqlite_dissect_linux-x64_onefile.spec
# Please see https://github.com/pyinstaller/pyinstaller/issues/5540 if errors with the ldconfig are encountered.
# -*- mode: python -*-

import PyInstaller.config

PyInstaller.config.CONF['distpath'] = "./dist/linux-x64/bin"

block_cipher = None


a = Analysis(['../main.py'],
             pathex=[],
             binaries=[],
             datas=[],
             hiddenimports=[],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          name='sqlite_dissect',
          debug=False,
          strip=False,
          upx=True,
          runtime_tmpdir=None,
          console=True )
