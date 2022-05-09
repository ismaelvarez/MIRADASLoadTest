#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import argparse
import yaml
import os
import datetime
import dateutil.parser
import time
import astropy.io.fits as fits
import numpy as np
# GCS python imports
from omniORB import CORBA
from gtc.DSL import DAF
import DGT
from gtc.DSL.DGCSTypes.IDL_Adapters import toIDL_TinyVector, toIDL_Frame
from DDPKF import DataElement
import DF
import DFAGENT
# MIRADAS python imports
from MIRADASDFAGENT import MIRADASDFAgent_ifce
import MIRADAS.MCS


class MIRADASDFAgentCommander:
    r"""Class to command the MIRADAS Data Factory Agent component.

    Type --help option for more information.

    """

    def __init__(self):
        r"""Constructor.

          Parameters
          ----------


          Returns
          -------
            None

        """
        # Define some common members
        self.instance_name = 'DF/MIRADASDFAgent'
        self.timeout = '30s'
        self.mechanism_positions = ('CW', 'HWP', 'QWP', 'MXS', 'WP', 'DM', 'FW', 'XD', 'DFS', 'DTS')
        self.reference = self.__obtain_reference(self.instance_name)

    def configure(self, instrument_mode, observation_type, observation_class, observation_mode, number_images):
        self.reference.setPrincipalInvestigatorName("Pepito")
        self.reference.setObserverName("Juanito")
        self.reference.setESOProgramId("ESO1")
        self.reference.setProgramId("P01")
        self.reference.setObservationBlockId("OB01")

        # Setup the instrument mode
        self.reference.setInstrumentMode(instrument_mode)

        # Setup the observation class
        print('Setup the observation class.')
        if observation_class == 'SCIENCE':
            self.reference.setObservationClass(DFAGENT.SCIENCE)
        elif observation_class == 'CALIB':
            self.reference.setObservationClass(DFAGENT.CALIB)

        self.reference.setImageTitle("Load test")

        self.reference.setObservationComment("Comment")

        # Setup the observation type
        print('Setup the observation type.')
        if observation_type == 'BIAS':
            self.reference.setObservationType(DFAGENT.BIAS)
        elif observation_type == 'DARK':
            self.reference.setObservationType(DFAGENT.DARK)
        elif observation_type == 'SKY_FLAT':
            self.reference.setObservationType(DFAGENT.SKY_FLAT)
        elif observation_type == 'DOME_FLAT':
            self.reference.setObservationType(DFAGENT.DOME_FLAT)
        elif observation_type == 'SPECTRAL_FLAT':
            self.reference.setObservationType(DFAGENT.SPECTRAL_FL)
        elif observation_type == 'ARC':
            self.reference.setObservationType(DFAGENT.ARC)
        elif observation_type == 'SKY':
            self.reference.setObservationType(DFAGENT.SKY)
        elif observation_type == 'OBJECTS':
            self.reference.setObservationType(DFAGENT.OBJECTS)

        self.reference.setObjectName("M3")

        # Setup the observation mode process
        print('Setup the observation mode.')
        self.reference.setObservationMode(observation_mode)

        # Setup number of images to collect
        print('Setup number of images to collect.')
        self.reference.setNumberOfFramesToCollect(number_images)

        # Setup timeout by each detector raw image
        # if args.timeout is not None:
        print('Setup timeout by image.')
        self.reference.setTimeoutByFrame(
            DGT.TimeValue(30000000)
        )

        # Setup current position for all mechanisms
        # if args.mechanism_positions is not None:
        print('Setup mechanism positions.')
        mechanisms = [
            (MIRADAS.MCS.COVER, "C"),
            (MIRADAS.MCS.HWPLATE, "HWP"),
            (MIRADAS.MCS.QWPLATE, "QWP"),
            (MIRADAS.MCS.MXS, "MXS"),
            (MIRADAS.MCS.WOLLASTON, "W"),
            (MIRADAS.MCS.DECKER, "D"),
            (MIRADAS.MCS.FILTER, "F"),
            (MIRADAS.MCS.DISPERSION, "DSP"),
            (MIRADAS.MCS.FOCUS, "FCS"),
            (MIRADAS.MCS.STAGE, "SGE"),
        ]
        for mechanism in mechanisms:
            print('Set positions of the {m} mechanism to {p}'.format(m=mechanism[0], p=mechanism[1]))
            self.reference.setMechanismPosition(*mechanism)

        self.reference.collectFrames()

    def send_images(self, images):
        r"""Send a set of images to the MIRADASDFAgent component.

          Parameters
          ----------
            reference : MIRADASDFAGENT.MIRADASDFAgent_ifce
                Python reference of MIRADASDFAgent.
            args : argparse.Namespace
                All parsed command line arguments.

          Returns
          -------
            None

        """
        # Send list of detector raw images
        if images is not None:
            # For each detector raw image
            for image in images:
                fileName = open(image, "r")
                # Declare all all parameters needed to invoke the receiveFrame command.
                idlFrame = None
                currentWindow = 1
                numWindows = 1
                idlBottomRight = None
                idlTimestamp = None
                idlExposureTime = None
                cameraId = 'MIRADAS'
                # Obtain all needed values from detector raw image to invoke properly the receiveFrame command.
                with fits.open(fileName) as hdulist:
                    idlFrame = toIDL_Frame(hdulist)
                    idlTopLeft = toIDL_TinyVector(np.array((0, 0)))
                    idlBottomRight = toIDL_TinyVector(
                        np.array(
                            (hdulist[0].header.get("NAXIS1"),
                             hdulist[0].header.get("NAXIS2"))
                        )
                    )
                    idlTimestamp = DGT.TimeValue(long(1e+6 * time.time()))
                    idlExposureTime = DGT.TimeValue(long(1e+6 * hdulist[0].header.get("EXPTIME", 0)))
                # Send a detector raw image
                print('Sending image {v} to DFAgent.'.format(v=fileName.name))
                self.reference.receiveFrame(
                    idlFrame,
                    currentWindow,
                    numWindows,
                    idlTopLeft,
                    idlBottomRight,
                    idlTimestamp,
                    idlExposureTime,
                    cameraId
                )
                # Delete all all parameters used to invoke the receiveFrame command.
                del idlFrame
                del currentWindow
                del numWindows
                del idlTopLeft
                del idlBottomRight
                del idlTimestamp
                del idlExposureTime
                del cameraId
                # Sleep
                tmp_sleep = float(hdulist[0].header.get("FRMTIME", 1e+3) / 1e+3)
                print('Sleep {v}.'.format(v=tmp_sleep))
                time.sleep(tmp_sleep)

                del tmp_sleep
        else:
            pass
            # raise ValueError('Error in {v} option'.format(v=args.images.name))
        return None

    @staticmethod
    def __obtain_reference(instance_name):
        r"""Obtain a Python reference of the MIRADASDFAgent component.

          Parameters
          ----------
            instance_name : str
                Instance name of MIRADSDFAgent.

          Returns
          -------
            reference : MIRADASDFAGENT.MIRADASDFAgent_ifce
                Python reference of MIRADASDFAgent.

        """
        # Obtain the reference to component.
        orb = CORBA.ORB_init(['-ORBgiopMaxMsgSize', '1048576000'], CORBA.ORB_ID)
        ns = DAF.GCSNameService(orb)
        reference = ns.resolve(instance_name, MIRADASDFAgent_ifce)
        del orb, ns
        # Make sure that the component is available.
        try:
            print('Ping component')
            reference.ping()
            print('The component is available')
        except CORBA.TRANSIENT:
            print('Caught exception while doing ping operation to {v} component.'.format(v=instance_name))
            # raise
            exit(code=-1)
        # Make sure that the component is in IDLE state.
        # if reference.isIdle():
        #     print('The {c} component is in IDLE'.format(c=instance_name))
        # else:
        #     print('The {c} component is not in IDLE'.format(c=instance_name))
        #     raise Warning('The {c} component is not in IDLE'.format(c=instance_name))
        # Display component version
        # print('The component state is {v}'.format(v=reference.state))
        # print('The component version is {v}'.format(v=reference.version))
        return reference

    def __set_timeout(self, args, timeout):
        r"""Sets the attribute 'timeout' in the Namespace, using the given value.

          The unit will be µseconds.

          Parameters
          ----------
            args : argparse.Namespace
                All parsed command line arguments.
            timeout : str
            String time representation.

          Returns
          -------
            None

        """
        setattr(args, 'timeout', timeout)
        # Parse timeout value
        tmp_datetime = dateutil.parser.parse(args.timeout)
        tmp_timedelta = datetime.timedelta(
            days=0,
            seconds=tmp_datetime.second,
            microseconds=tmp_datetime.microsecond,
            milliseconds=0,
            minutes=tmp_datetime.minute,
            hours=tmp_datetime.hour,
            weeks=0
        )
        del tmp_datetime
        args.timeout = long(1e+6 * tmp_timedelta.total_seconds())  # Convert to µsecond
        del tmp_timedelta


class Utilities:
    @staticmethod
    def get_random_images(folder, num_images):
        images = []
        files = os.listdir(folder)
        for image in files:
            # check only text files
            if image.endswith('.fits') and "raw" in image:
                images.append(os.path.join(folder, image))

            if len(images) == num_images:
                return images
        return images

    @staticmethod
    def print_command(command):
        print("==============================================================================")
        print(command)
        print("==============================================================================")


class Config(object):
    def __init__(self):
        self.instrument_mode = None
        self.observation_type = None
        self.observation_mode = None
        self.observation_class = None
        self.image_path = None
        self.number_images = None
        self.number_petitions = None
        self.petition_period = None


class DFAgentLoadTest:
    def __init__(self):
        self.commander = MIRADASDFAgentCommander()
        self.utilities = Utilities()
        self.args = None

    def parse(self, argv):

        parser = argparse.ArgumentParser(
            description='This script allows to command an instance of DFAgent, using python.'
        )

        parser = argparse.ArgumentParser(
            description='This script allows to command an instance of DFAgent, using python.'
        )
        # Define commands
        subparsers = parser.add_subparsers(title='commands', dest='command', help='Command to run')

        command_parser = subparsers.add_parser(
            'command',
            help='Set parameters from commandline'
        )

        file_parser = subparsers.add_parser(
            'file',
            help='Set parameters from config file.'
        )
        args = parser.parse_args(argv[0:1])

        method = '_' + self.__class__.__name__ + '__parse_' + args.command

        if not hasattr(self, method):
            parser.print_help()
            print('Unrecognized command')
            exit(code=-1)
        # Create a map of parsers.
        parser_map = {
            'command': command_parser,
            'file': file_parser,
        }
        # Invoke method with same name, using the dispatch pattern.
        getattr(self, method)(parser_map[args.command], argv[1:], args)
        # Parse command

    def __parse_command(self, parser, argv, namespace):
        r"""Defines and parse the command line arguments .

          Parameters
          ----------
            parser : argparse.ArgumentParser
                Argument parser for configure command.
            argv : list of str
                Command line arguments related to configure command.
            namespace : argparse.Namespace
                Object where all inherited attributes are holding.

          Returns
          -------
            None

        """

        parser.add_argument(
            '--instrument-mode',
            type=str,
            required=False,
            dest='instrument_mode',
            help='Determine the instrument mode of DFAgent.'
        )

        parser.add_argument(
            '--observation-class',
            type=str,
            choices=['SCIENCE', 'CALIB'],
            required=False,
            dest='observation_class',
            help='Determine the observation class.'
        )

        parser.add_argument(
            '--observation-type',
            type=str,
            choices=['BIAS', 'DARK', 'SKY_FLAT', 'DOME_FLAT', 'SPECTRAL_FLAT', 'ARC', 'SKY', 'OBJECTS'],
            required=False,
            dest='observation_type',
            help='Determine the observation type.'
        )

        parser.add_argument(
            '--observation-mode',
            type=str,
            required=False,
            dest='observation_mode',
            help='Determine the observation mode process to be used.'
        )

        parser.add_argument(
            '--image-path',
            type=str,
            required=False,
            dest='image_path',
            default=".",
            help='Image folder to send to DFAgent component.'
        )

        parser.add_argument(
            '--number-images',
            type=int,
            required=True,
            dest='number_images',
            default=1,
            help='Determine the number of images sent in to DFAgent.'
        )

        parser.add_argument(
            '--number-petitions',
            type=int,
            required=True,
            dest='number_petitions',
            help='Determine the number of petitions to DFAgent.'
        )

        parser.add_argument(
            '--petition-period',
            type=float,
            required=True,
            dest='petition_period',
            help='Determine the petition period to DFAgent.'
        )

        args = parser.parse_args(argv, namespace)

        self.args = args

    def __parse_file(self, parser, argv, namespace):
        parser.add_argument(
            '--config-file',
            type=argparse.FileType('r'),
            required=False,
            dest='config_file',
            help='Determine the config file location.'
        )

        args = parser.parse_args(argv, namespace)

        configuration = yaml.load(args.config_file)

        print(configuration['configuration'])

        c = Config()
        for key in configuration['configuration']:
            setattr(c, key, configuration['configuration'][key])

        self.args = c

    def start(self):
        if self.args is not None:
            print("Generating %d petitions with %d images. " % (self.args.number_petitions, self.args.number_images))
            for i in range(self.args.number_petitions):
                self.configure()
                self.send()
                time.sleep(self.args.petition_period)

    def configure(self):

        self.commander.configure(self.args.instrument_mode, self.args.observation_type,
                                 self.args.observation_class, self.args.observation_mode, self.args.number_images)

    def send(self):

        images = self.utilities.get_random_images(self.args.image_path, self.args.number_images)
        self.commander.send_images(images)


def main(argv):
    r"""Main function

      Parameters
      ----------
        argv : list of str
            Command line arguments, where argv[0] is the script pathname if known.

      Returns
      -------
        None
    """

    df_tester = DFAgentLoadTest()
    df_tester.parse(argv)
    df_tester.start()


if __name__ == '__main__':
    main(sys.argv[1:])
