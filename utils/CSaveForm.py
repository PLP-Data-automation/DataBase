"""
Author: Fuentes Juvera, Luis
E-mail: luis.fuju@outlook.com
username: LuisDFJ

CSaveForm Module: Creates a dialog for selecting files.

Propmps a file explorer window to select .csv files or
open a directory.

Functions
---------
Dialog( mode : str = 'file' ) -> list[ str ]

"""

from PyQt5 import QtCore, QtWidgets, QtGui
        
def Dialog( mode : str = 'file' ) -> list:
    """
    Dialog box wrapper for file explorer.

    Returns
    -------
    list
        Containing path to all files.

    """
    app = QtWidgets.QApplication( [] )
    dialog = QtWidgets.QFileDialog()
    if mode == 'file':
        dialog.setFileMode( QtWidgets.QFileDialog.FileMode.ExistingFiles )
        dialog.setNameFilter( "Comma Separated Value (*.csv)" )
    elif mode == 'dir':
        dialog.setFileMode( QtWidgets.QFileDialog.FileMode.Directory )
    dialog.exec()
    return dialog.selectedFiles()

