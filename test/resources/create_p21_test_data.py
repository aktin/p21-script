# -*- coding: UTF-8 -*-.

import csv
import os
import random
from zipfile import ZipFile

import numpy as np

NUM_PATIENTS = 4000
NUM_FACTS = NUM_PATIENTS * 2

CODES_ICD = ['S82.31', 'I25.19', 'K62.6', 'R60.1', 'J45.8', 'S70.0', 'S32.1', 'G81.0', 'Q45.3', 'Z86.7', 'J44.89', 'F19.1', 'K75.9', 'T85.78', 'Z43.5', 'R26.0', 'N10', 'T81.0', 'R26.8', 'K29.1',
             'O91.20', 'G82.09', 'K55.31', 'A08.3', 'I70.20', 'P21.1', 'Z12.1', 'S02.8', 'K56.4', 'S32.02', 'Z99.8', 'M54.6', 'S60.81', 'G20.00', 'O64.1', 'K57.30', 'L02.2', 'I42.88', 'S52.30',
             'Z88.6', 'K40.41', 'O70.1', 'K61.0', 'C79.5', 'M62.80', 'K81.1', 'I25.20', 'R53', 'L89.34', 'I06.8', 'F42.1', 'S52.50', 'K63.5', 'I36.1', 'Z03.2', 'K55.21', 'P07.12', 'Z97.8', 'I63.0',
             'F60.6', 'I49.8', 'P28.4', 'S42.22', 'Z29.21', 'I00', 'S30.1', 'F25.1', 'I44.7', 'H65.0', 'E78.0', 'O68.1', 'I46.0', 'E16.1', 'S43.01', 'K56.6', 'I25.13', 'D41.0', 'N81.3', 'I51.7',
             'K80.00', 'S32.05', 'I11.00', 'K92.1', 'T82.7', 'F10.3', 'G40.8', 'K85.00', 'A87.8', 'Z47.0', 'R30.0', 'S01.1', 'N88.4', 'M48.02', 'N28.1', 'Z95.2', 'I21.3', 'J45.1', 'S82.0', 'J20.5',
             'I50.01', 'M47.27', 'I69.2', 'H61.0', 'A09.9', 'I63.8', 'S22.42', 'K22.81', 'T20.0', 'Z03.4', 'D50.0', 'K61.1', 'S52.59', 'I89.00', 'F50.00', 'G35.10', 'B02.2', 'C78.7', 'S42.21',
             'I69.3', 'J03.8', 'Z43.1', 'S00.85', 'K25.0', 'C83.1', 'C91.10', 'Z98.0', 'C45.0', 'E10.91', 'R17.0', 'K29.7', 'M48.09', 'C50.9', 'G62.9', 'K85.10', 'B34.9', 'G44.2', 'P38', 'T18.1',
             'I67.2', 'I83.1', 'I25.3', 'K83.1', 'N48.1', 'N98.1', 'E10.61', 'I48.3', 'P92.0', 'S72.11', 'F45.34', 'C19', 'Z95.5', 'I11.90', 'B36.8', 'U50.30', 'S80.0', 'Z96.64', 'R52.0', 'F06.9',
             'H49.0', 'Z08.8', 'A84.1', 'D68.9', 'K35.8', 'K76.6', 'R68.8', 'C18.2', 'D39.0', 'A87.9', 'I35.0', 'I25.8', 'I49.3', 'Z95.4', 'B34.8', 'S72.01', 'K21.0', 'S42.3', 'T87.5', 'S32.03',
             'H53.2', 'N94.4', 'N30.0', 'G51.0', 'N80.0', 'I62.02', 'O36.8', 'G83.8', 'H40.9', 'F44.5', 'L89.15', 'I48.2', 'G40.2', 'O06.9', 'K29.0', 'D37.6', 'K25.7', 'D62', 'T81.3', 'I50.9',
             'G93.1', 'Z74.1', 'K80.01', 'O32.1', 'R94.3', 'G47.31', 'S52.6', 'A04.5', 'F05.1', 'I63.1', 'O66.2', 'Z96.65', 'N13.3', 'Z01.80', 'K22.2', 'T84.04', 'Z92.2', 'K59.09', 'K57.90', 'R41.3',
             'Z29.0', 'K52.1', 'K29.5', 'A49.9', 'I95.1', 'M65.11', 'N81.5', 'O14.1', 'L03.02', 'K63.1', 'G35.31', 'Z39.0', 'R20.8', 'E79.0', 'B37.0', 'G12.2', 'C54.1', 'C79.3', 'E11.50', 'I77.1',
             'J20.8', 'A08.1', 'G83.6', 'F60.31', 'E66.82', 'K81.0', 'O82', 'P70.0', 'Z87.6', 'M17.3', 'H61.2', 'M35.1', 'R06.4', 'M19.84', 'C30.0', 'I25.11', 'S37.28', 'F70.0', 'K26.0', 'I07.1',
             'E06.3', 'C54.9', 'Z38.1', 'K80.51', 'Z45.01', 'C80.0', 'I42.1', 'J43.8', 'C25.8', 'S42.24', 'M81.90', 'F43.2', 'D13.2', 'I95.8', 'S52.51', 'G57.3', 'K20', 'O70.2', 'E89.2', 'R47.0',
             'O14.2', 'S32.7', 'D18.02', 'Z87.3', 'D12.4', 'O26.88', 'T85.82', 'F05.0', 'Z13.8', 'K85.80', 'F50.9', 'I63.3', 'F03', 'N18.4', 'N13.2', 'P59.0', 'T85.73', 'F33.1', 'N17.93', 'K71.9',
             'S32.3', 'N85.0', 'O70.0', 'R41.0', 'R20.2', 'Z43.2', 'S82.7', 'Z03.3', 'K76.0', 'C34.1', 'R51', 'E66.00', 'I42.9', 'R11', 'G25.2', 'M80.88', 'D12.6', 'K70.3', 'M16.1', 'O28.8', 'R47.8',
             'I35.2', 'O62.2', 'S32.4', 'S36.04', 'K43.0', 'Z89.4', 'R06.80', 'S52.01', 'G30.1', 'I48.0', 'D50.9', 'F45.0', 'K62.1', 'E78.1', 'N60.1', 'E86', 'G83.2', 'H53.8', 'K80.30', 'I21.2',
             'D69.80', 'I20.8', 'E87.5', 'P29.8', 'B02.3', 'K57.31', 'H60.8', 'F41.1', 'C65', 'Z04.2', 'M84.45', 'M51.1', 'L97', 'M13.0', 'I48.1', 'K40.91', 'K29.4', 'E66.01', 'G45.33', 'G93.0',
             'R25.3', 'I12.00', 'C18.4', 'M54.16', 'E78.2', 'I50.12', 'G03.8', 'E11.90', 'S42.29', 'M48.07', 'F45.1', 'P07.11', 'S22.03', 'I25.12', 'I61.3', 'E11.40', 'I80.20', 'J69.0', 'I49.4',
             'M54.89', 'D50.8', 'J45.0', 'N80.2', 'K64.2', 'E05.9', 'J18.2', 'F10.0', 'K55.22', 'I25.16', 'G40.1', 'H81.2', 'B01.1', 'N73.6', 'K22.5', 'R57.2', 'S82.81', 'H81.3', 'J44.10', 'I25.4',
             'F45.8', 'I87.21', 'M72.2', 'K83.4', 'K31.81', 'O69.8', 'J18.0', 'K80.41', 'O85', 'N63', 'S70.1', 'K42.9', 'M54.2', 'N39.0', 'R60.0', 'R57.0', 'N40', 'T88.6', 'C18.9', 'I70.29', 'E66.80',
             'K80.21', 'M50.0', 'I67.3', 'B37.81', 'L90.5', 'G93.6', 'M06.99', 'R63.6', 'O68.0', 'K70.0', 'D44.1', 'R40.1', 'R50.9', 'D69.59', 'C34.9', 'E05.2', 'M17.1', 'F50.1', 'S93.30', 'K92.2',
             'R59.0', 'T62.8', 'K35.31', 'S46.0', 'R07.0', 'J15.9', 'R25.1', 'O06.1', 'D65.1', 'M87.85', 'S02.2', 'S22.06', 'K40.30', 'S22.32', 'G25.0', 'M13.84', 'S62.62', 'T85.6', 'G31.0', 'Z88.0',
             'J44.00', 'S52.4', 'G30.8', 'S42.12', 'Z85.0', 'H49.1', 'E66.02', 'F17.2', 'I42.0', 'E87.2', 'J02.9', 'I49.0', 'I21.1', 'R07.2', 'K40.90', 'K31.1', 'N39.3', 'M60.80', 'R55', 'K29.8',
             'O00.1', 'J15.8', 'G47.38', 'P70.4', 'R29.5', 'J20.9', 'H81.1', 'T79.68', 'I51.3', 'R33', 'K59.9', 'E89.0', 'Z96.0', 'S06.5', 'R12', 'I40.8', 'Z92.3', 'D69.61', 'K14.8', 'A05.8', 'Q24.0',
             'Z51.1', 'I35.1', 'J95.0', 'S06.4', 'R00.1', 'S42.02', 'F34.1', 'I27.28', 'I70.22', 'J96.01', 'M89.55', 'P07.3', 'N93.8', 'M25.51', 'E16.0', 'Z99.2', 'I48.4', 'I25.5', 'Q89.8', 'F41.8',
             'R44.1', 'I26.9', 'E87.0', 'D37.1', 'Z51.2', 'P39.9', 'E85.4', 'F05.8', 'I25.10', 'S72.04', 'K41.90', 'N80.3', 'H47.1', 'M86.35', 'B99', 'R56.0', 'R10.0', 'S51.80', 'F79.9', 'M48.06',
             'S42.01', 'R29.2', 'K91.3', 'I44.0', 'G43.1', 'F40.1', 'Z43.3', 'S82.18', 'Z74.0', 'E87.1', 'I50.13', 'I22.1', 'Z90.5', 'D64.8', 'C82.0', 'S27.31', 'D37.4', 'Z04.1', 'J44.99', 'I34.88',
             'G41.8', 'M06.90', 'M51.2', 'J96.09', 'T17.5', 'M41.55', 'K83.0', 'E03.9', 'R00.0', 'S22.44', 'Z45.08', 'T81.8', 'S02.60', 'I10.00', 'P22.8', 'R45.1', 'S30.0', 'I46.9', 'T89.02', 'Z90.1',
             'T75.4', 'D43.4', 'L23.9', 'Z45.00', 'G45.42', 'Z89.5', 'O60.1', 'P96.1', 'M25.56', 'S05.8', 'S80.81', 'K50.9', 'R58', 'E11.41', 'S02.1', 'O99.0', 'Z22.8', 'S01.41', 'Z48.0', 'G23.3',
             'H47.0', 'F14.5', 'R14', 'K51.0', 'R07.3', 'E53.8', 'K63.3', 'J84.1', 'N17.92', 'G93.80', 'K65.00', 'I60.8', 'R26.3', 'R56.8', 'G82.59', 'I25.14', 'R10.4', 'I60.6', 'J44.19', 'E78.5',
             'G20.11', 'D12.5', 'Z85.3', 'S32.01', 'F33.2', 'A46', 'T88.7', 'F45.40', 'I10.01', 'D61.9', 'R63.8', 'C90.00', 'I34.0', 'K40.31', 'P28.2', 'E66.81', 'C18.0', 'G40.5', 'D27', 'S06.33',
             'G50.0', 'I44.1', 'R64', 'A49.8', 'I11.91', 'R23.3', 'M10.97', 'Z88.8', 'D68.4', 'J44.80', 'J21.0', 'S72.3', 'K62.5', 'Z85.5', 'S13.4', 'D25.9', 'S50.81', 'I26.0', 'C24.0', 'R05',
             'P39.8', 'K65.8', 'H53.4', 'R22.7', 'T78.0', 'R10.1', 'Q21.2', 'Z92.1', 'S31.0', 'K66.0', 'S01.80', 'Z90.7', 'Z94.4', 'O42.0', 'R06.88', 'S81.80', 'S52.21', 'J12.8', 'J00', 'O48',
             'G70.0', 'G06.0', 'S72.10', 'E88.0', 'I61.0', 'Z87.1', 'R74.8', 'N71.0', 'M75.1', 'C50.5', 'H34.2', 'G97.1', 'P37.8', 'T79.69', 'O14.0', 'M54.17', 'C73', 'F33.8', 'G40.3', 'S00.1',
             'Z38.0', 'G20.90', 'D41.4', 'Z11', 'K57.22', 'C77.3', 'S22.05', 'R62.8', 'J96.19', 'O80', 'B86', 'D12.3', 'I50.14', 'Z95.81', 'O41.1', 'M24.61', 'R91', 'C16.3', 'N80.4', 'R04.0',
             'S52.11', 'Z95.88', 'A09.0', 'C86.0', 'R40.0', 'T84.14', 'Z86.1', 'C50.2', 'E11.91', 'K51.9', 'R04.8', 'H57.0', 'K40.21', 'S00.81', 'I31.3', 'I21.0', 'J06.9', 'F33.9', 'K56.0', 'J98.0',
             'D38.1', 'A41.9', 'E11.74', 'A49.0', 'I67.88', 'I89.1', 'N18.3', 'R47.1', 'J96.00', 'N92.0', 'I63.4', 'G83.1', 'I83.9', 'K57.32', 'K43.2', 'J90', 'K92.0', 'S40.0', 'O64.4', 'D17.6',
             'D68.5', 'M54.4', 'I95.2', 'T78.3', 'J38.5', 'K22.1', 'E03.8', 'D68.35', 'Z91.1', 'M50.2', 'N17.91', 'S86.0', 'F10.2', 'K35.32', 'G40.9', 'E21.0', 'K82.3', 'T80.2', 'P36.9', 'M06.89',
             'I34.1', 'Z22.3', 'D69.53', 'M19.01', 'G45.92', 'E04.2', 'Z43.6', 'S46.2', 'K56.5', 'S93.0', 'Z99.1', 'P29.1', 'F61', 'D70.10', 'S32.04', 'Z95.0', 'T78.1', 'S42.20', 'I82.80', 'I25.15',
             'Z03.8', 'R06.0', 'K80.11', 'G25.3', 'G25.81', 'R07.4', 'S32.5', 'C50.4', 'R13.0', 'K40.20', 'E11.75', 'S42.19', 'Z00.1', 'M79.65', 'S01.0', 'K29.6', 'M43.16', 'C20', 'N20.0', 'N80.1',
             'S42.41', 'Z95.1', 'Z43.0', 'D65.2', 'I11.01', 'K26.3', 'S43.1', 'J44.09', 'I20.0', 'S52.8', 'F32.1', 'K64.0', 'R15', 'T88.8', 'N99.4', 'M35.3', 'Z51.83', 'J21.9', 'E23.0', 'S06.0',
             'I48.9', 'D70.3', 'R13.9', 'J98.1', 'S06.6', 'M75.5', 'K60.0', 'Z34', 'F12.0', 'R73.0', 'I95.9', 'R63.4', 'K50.0', 'C51.9', 'F45.41', 'E05.8', 'B02.0', 'S72.2', 'J86.9', 'K80.20',
             'B18.12', 'O34.38', 'E05.0', 'D25.1', 'R41.2', 'L02.4', 'M00.94', 'S20.2', 'S62.52', 'G06.1', 'N62', 'N81.6', 'M25.57', 'M46.44', 'K76.1', 'R00.2', 'D68.33', 'C78.6', 'T80.8', 'L89.14',
             'R50.2', 'U50.40', 'S02.0', 'O60.0', 'Z49.1', 'P59.8', 'H55', 'R52.1', 'Z23.5', 'N83.2', 'E78.4', 'J45.9', 'J18.9', 'T78.2', 'S52.52', 'K50.1', 'C67.0', 'R72', 'I49.5', 'I45.1', 'L22',
             'F45.30', 'C56', 'Z95.3', 'I69.1', 'R18', 'C50.8', 'C25.3', 'L89.01', 'Z03.1', 'M79.70', 'K44.9', 'I50.11', 'O21.0', 'M54.5', 'D35.0', 'H54.0', 'I25.22', 'C16.9', 'F32.9', 'Z92.6',
             'O46.8', 'E66.92', 'Z04.8', 'D43.1', 'S50.0', 'Q21.1', 'E83.58', 'Z90.4', 'S20.41', 'T84.20', 'D47.2', 'R50.80', 'D64.9', 'G81.9', 'F54', 'A08.4', 'J18.8', 'C18.8', 'S12.1', 'K56.7',
             'N18.5', 'T81.4', 'R06.1', 'R42', 'T30.0', 'A08.2', 'K72.0', 'N84.0', 'Z87.4', 'S00.05', 'C25.2', 'K07.2', 'I80.28', 'O13', 'K35.30', 'D06.1', 'R57.8', 'S63.4', 'M17.5', 'N81.1', 'K52.8',
             'C25.0', 'I47.2', 'K22.6', 'F10.1', 'D12.0', 'C34.8', 'C53.0', 'G82.39', 'Z03.5', 'L29.8', 'D39.1', 'J39.88', 'K91.0', 'N99.3', 'K80.10', 'R48.2', 'I25.21', 'A41.51', 'C51.8', 'I65.2',
             'M50.1', 'Z88.1', 'E87.6', 'Z90.3', 'M75.4', 'K74.6', 'R29.6', 'F32.8', 'Z85.4', 'I21.4', 'Z75.8', 'E10.11', 'G82.49', 'D12.8', 'T80.1', 'S62.33', 'Z89.7', 'N61', 'J95.80', 'K40.00',
             'F01.8', 'R31', 'J43.2', 'L89.27', 'J93.0', 'I44.2', 'F60.30', 'Z51.0', 'T85.74', 'N39.48', 'F43.1', 'M70.2', 'A69.2', 'I70.25', 'F32.2', 'C18.6', 'G82.13', 'O64.0', 'Z32', 'R20.1',
             'K86.1', 'R10.3', 'Q43.0', 'Z93.3', 'F44.6', 'Z73', 'I34.80', 'Z96.4', 'S82.82', 'C16.8', 'C22.0', 'I47.1', 'K51.5', 'I61.1', 'O81', 'C25.9', 'R04.2', 'E04.9', 'T17.9', 'K80.50', 'N19',
             'K25.3']

CODES_OPS = ['5452a0', '3990', '593243', '81521', '584435', '8837m9', '57281', '15025', '541301', '590004', '583651', '549300', '15593', '37220', '544551', '53771', '12661', '545571', '584503',
             '550151', '891412', '589415', '5389ax', '57941n', '14422', '56900', '5690x', '5986x', '59000a', '588930', '964953', '57910g', '883533', '58390', '5839a0', '883bb6', '3221', '5796k0',
             '554321', '54321', '5822g1', '58125', '57060', '58213x', '99847', '964952', '3202', '8839x', '9261', '30551', '3800', '199511', '95000', '1440a', '586a3', '58143', '557801', '12071',
             '8153', '12082', '883521', '819020', '553436', '12651', '58526a', '54692x', '545545', '550120', '593212', '52100', '57908e', '5794k6', '579314', '55364a', '58665', '58310',
             '55142x', '8191x', '583701', '964914', '579013', '581120', '58299', '589617', '54900', '546452', '54661', '58920a', '9984a', '16432', '3605', '57806f', '540612', '1265e', '92620',
             '57806g', '964916', '553931', '3228', '3801', '50122', '8607x', '565762', '12683', '14446', '593214', '55001', '3604', '57900n', '8800g2', '8835a5', '5832x', '81231', '55905x', '50357',
             '53204', '554341', '5406a', '12042', '89310', '5513a', '37051', '1279a', '581130', '57931r', '14814', '57905f', '883530', '964931', '5787kx', '143201', '38240', '579427', '54060',
             '568192', '57942r', '56586', '1651', '3991', '58324', '5841a4', '503210', '16520', '537771', '8706', '565332', '313b', '1206', '585918', '535a41', '8837s0', '8837d0',
             '8837m3', '8837m5', '57848x', '535a61', '57906n', '565192', '53200', '819021', '82010', '871100', '50368', '545260', '503211', '898f11', '583631', '581020', '898123', '545410', '57561',
             '545171', '54662', '5787k1', '568190', '58961d', '883d6', '582129', '143030', '502910', '12654', '3051', '574442', '5786g', '5916a1', '58692', '50628', '964920', '16433', '81270', '313a',
             '55471', '5995', '535a60', '546910', '59000d', '582201', '883cfb', '5719', '50612', '5870a2', '305g0', '16300', '583b52', '54352', '8919', '5639x', '5794k4', '5983', '545272', '891401',
             '53869d', '12754', '5449s3', '545173', '14712', '57901b', '87001', '3225', '501004', '579041', '3821', '964910', '565772', '964921', '5794k1', '546700', '12681', '583962', '883701',
             '593270', '55341', '545521', '8810j3', '15024', '583961', '57906d', '14406', '3226', '85101', '12732', '583b70', '589207', '81921b', '585519', '99849', '589608', '57932n', '3220',
             '8837m2', '58591x', '199900', '3802', '12751', '545190', '58810', '3828', '14650', '58791', '5691', '58003g', '5543y', '5756x', '16910', '57900x', '57400', '81004x', '898710', '88360s',
             '553610', '546900', '18532', '5429a', '16502', '8903', '8924', '50630', '81442', '554342', '12081', '55131', '582051', '871871', '5796g0', '551141', '86401', '1447', '1655', '579610',
             '80208', '163a', '54512', '3996', '5870a5', '50210', '582041', '537730', '55450', '57941j', '568323', '574402', '553131', '9634', '14409', '545280', '58004k', '12684', '57942j', '57874n',
             '57879n', '53785c', '55412', '58316', '585518', '57280', '58491', '964950', '852291', '582125', '16200x', '503212', '553031', '50151', '57861', '57941f', '57940n', '5986y', '3035',
             '1843', '556931', '50408', '582002', '5069x', '5829a', '55132x', '898121', '53161', '566142', '57818k', '58657', '5787y', '567103', '883700', '8837m0', '590017', '544973', '38430',
             '883bbd', '5449d3', '81006', '55495', '86400', '541300', '3137', '16541', '58147', '598c2', '57865', '1672', '559320', '54069', '88315', '12752', '81762', '964912', '883bc2', '12021',
             '898f20', '503230', '5791kq', '87130', '5850f8', '3203', '5794kr', '579a0e', '8837m4', '883bb7', '883bba', '99846', '579520', '548211', '58317', '883b0c', '57903f', '548292', '5793af',
             '545541', '12755', '57832d', '58645', '8800c0', '12660', '57913g', '59883', '16521', '579axk', '553073', '15510', '15046', '58921b', '8835j', '583700', '964932', '58001g',
             '16320', '52992', '5835b0', '81760', '3804', '552401', '8810x', '305f', '57873r', '81440', '5513f1', '57875f', '593244', '80103', '3034', '883946', '583bx0', '542252', '8200n', '1640',
             '57024', '1265f', '5787xx', '570400', '3205', '16500', '5850da', '5452a1', '583b71', '582001', '540111', '162000', '543451', '8500', '546701', '5513f0', '53997', '12657', '5794kn',
             '570411', '57585', '5793b1', '5794k7', '551320', '546131', '582021', '570401', '585581', '503130', '545264', '58307', '56815x', '1646', '8201k', '547011', '503200', '5984', '854313',
             '1791', '57022', '57401', '8933', '162001', '57380', '50352', '883bc6', '545575', '8800c1', '5401a0', '898f0', '3054', '8179x', '81441', '92800', '8771', '16360', '540100', '964940',
             '881250', '578196', '5469j3', '89171x', '57905g', '542221', '55721', '58633', '546911', '54912', '600938', '16501', '553000', '5870a0', '57933n', '583963', '5795g0', '57940r', '88310',
             '883b08', '12743', '545420', '579b21', '541311', '553071', '5489d', '883520', '1661', '54063', '964954', '553403', '6001c4', '16310', '8176x', '58321', '9607', '5829h', '543120', '3805',
             '382a', '81271', '582000', '85471', '1694', '546903', '58842', '58740', '883522', '19990x', '583960', '57490', '964942', '568150', '92621', '543321', '8930', '578717', '12750', '57877f',
             '3053', '556930', '1424', '545576', '58920d', '883534', '5469d3', '578761', '58921a', '1559x', '579065', '55410', '81480', '96490', '14420', '55301', '53453', '5793k6', '5805x', '3607',
             '501000', '58464', '964951', '58395', '583650', '50131', '579016', '898f10', '3900', '546010', '14447', '574910', '8642', '50315', '3992', '881251', '50130', '553649', '546921', '964513',
             '5513q0', '55420', '5322h4', '55242', '553003', '964911', '5794af', '3808', '964941', '545262', '88110', '14720', '545261', '898g11', '3222', '8835a3', '81320', '1642', '883535',
             '964930', '3030', '545265', '5469u3', '58458', '99848', '5702x', '568202', '3201', '58811', '57044c', '503071', '53995', '31001', '552621', '8836ma', '57931n', '57942h', '86070',
             '579600', '3052', '57862', '551321', '5787k6', '58359', '12680', '580047', '566340', '5513b', '8837m7', '593215', '54062', '5513d', '593211', '15594', '57903n', '583b32', '940130',
             '58057', '5800ax', '546920', '593222', '57044a', '6004d', '54511', '852290', '964933', '583b54', '554320', '8800g1', '503102', '579321', '583b72', '5489e', '143020', '57879m', '582421',
             '503231', '5730', '8200r', '883948', '12731', '58312', '546930', '8836n3', '5786k', '57900e', '579a6k', '5322g1', '585912', '88542', '57860', '883532', '8837q', '8837m1', '12070',
             '56837', '8835h', '12086', '59001b', '57933r', '5546y', '3056', '9260', '565682', '58721', '58890', '5658y', '53776', '57942n', '898120', '50610', '5546x', '81761', '883750', '3994',
             '8810j9', '593213', '506940', '305a', '5850d0', '58961b', '583w0', '58072', '3825', '570410', '5469w3', '50341', '8910', '551111', '503070', '50102', '143200', '8701', '58000g', '568320',
             '9984b', '3820', '503072', '30550', '964956', '3823', '898122', '5791kg', '88000', '5471x', '127621', '5700', '545525', '548231', '566162', '883990', '566160', '545282', '3206',
             '579128', '3200', '58920x', '1653', '5916a0', '88358', '8390x', '881252', '964934', '1844', '5449v3', '56814', '5340b', '14263', '12744', '964913', '384x', '583b30', '574911',
             '3207', '85602', '546702']


def generate_random_date(year: str):
    month = str(np.random.randint(1, 12))
    if len(month) < 2:
        month = '0' + month
    day = str(np.random.randint(1, 26))
    if len(day) < 2:
        day = '0' + day
    hour = str(np.random.randint(0, 23))
    if len(hour) < 2:
        hour = '0' + hour
    minute = str(np.random.randint(0, 59))
    if len(minute) < 2:
        minute = '0' + minute
    return ''.join([year, month, day, hour, minute])


def save_test_data_as_csv_to_local_folder(name_csv: str, dict_csv: dict):
    path = os.path.join(os.getcwd(), name_csv)
    with open(path, 'w', newline='\n') as output:
        writer = csv.writer(output, delimiter=';')
        writer.writerow(dict_csv.keys())
        writer.writerows(zip(*[dict_csv[key] for key in dict_csv.keys()]))


def create_test_FALL_max() -> dict:
    fall = {'IK':                                      ['261700001' for _ in range(NUM_PATIENTS)],
            'Entlassender-Standort':                   ['770001000' for _ in range(NUM_PATIENTS)],
            'Entgeltbereich':                          random.choices(['DRG', 'PSY'], weights=[0.66, 0.3], k=NUM_PATIENTS),
            'KH-internes-Kennzeichen':                 [x + 1000 for x in range(NUM_PATIENTS)],
            'Versicherten-ID':                         ['9999999999' for _ in range(NUM_PATIENTS)],
            'Vertragskennzeichen-64b-Modellvorhaben':  ['' for _ in range(NUM_PATIENTS)],
            'IK-der-Krankenkasse':                     ['161556856' for _ in range(NUM_PATIENTS)],
            'Geburtsjahr':                             np.random.randint(1900, 2020, NUM_PATIENTS),
            'Geburtsmonat':                            np.random.randint(1, 12, NUM_PATIENTS),
            'Geschlecht':                              random.choices(['m', 'w', 'd', 'x', ''], k=NUM_PATIENTS),
            'PLZ':                                     np.random.randint(10000, 99999, NUM_PATIENTS),
            'Wohnort':                                 ['Musterstadt' for _ in range(NUM_PATIENTS)],
            'Aufnahmedatum':                           [generate_random_date('2020') for _ in range(NUM_PATIENTS)],
            'Aufnahmeanlass':                          random.choices(['E', 'Z', 'N', 'R', 'V', 'A', 'G', 'B'], k=NUM_PATIENTS),
            'Aufnahmegrund':                           ['0' + str(np.random.randint(100, 799)) for _ in range(NUM_PATIENTS)],
            'Fallzusammenführung':                     random.choices(['J', 'N', ''], weights=[0.5, 0.25, 0.25], k=NUM_PATIENTS),
            'Fallzusammenführungsgrund':               random.choices(['OG', 'MD', 'KO', 'RU', 'WR', 'MF', 'PW', 'PM', 'ZW', 'ZM', ''], k=NUM_PATIENTS),
            'Entlassungsdatum':                        [generate_random_date('2021') for _ in range(NUM_PATIENTS)],
            'Entlassungsgrund':                        [np.random.randint(100, 999) for _ in range(NUM_PATIENTS)],
            'Alter-in-Tagen-am-Aufnahmetag':           ['0' for _ in range(NUM_PATIENTS)],
            'Alter-in-Jahren-am-Aufnahmetag':          ['' for _ in range(NUM_PATIENTS)],
            'Aufnahmegewicht':                         ['0' for _ in range(NUM_PATIENTS)],
            'Patientennummer':                         ['P' + str(x + 1000) for x in range(NUM_PATIENTS)],
            'Interkurrente-Dialysen':                  ['' for _ in range(NUM_PATIENTS)],
            'Beatmungsstunden':                        [np.random.randint(0, 10) for _ in range(NUM_PATIENTS)],
            'Behandlungsbeginn-vorstationär':          ['20190101' for _ in range(NUM_PATIENTS)],
            'Behandlungstage-vorstationär':            [np.random.randint(0, 365) for _ in range(NUM_PATIENTS)],
            'Behandlungsende-nachstationär':           ['20220101' for _ in range(NUM_PATIENTS)],
            'Behandlungstage-nachstationär':           [np.random.randint(0, 365) for _ in range(NUM_PATIENTS)],
            'IK-Verlegungs-KH':                        ['' for _ in range(NUM_PATIENTS)],
            'Belegungstage-in-anderem-Entgeltbereich': ['0' for _ in range(NUM_PATIENTS)],
            'Beurlaubungstage-PSY':                    ['0' for _ in range(NUM_PATIENTS)],
            'Kennung-Besonderer-Fall-Modellvorhaben':  ['' for _ in range(NUM_PATIENTS)],
            'Verweildauer-Intensiv':                   [np.random.randint(0, 10) for _ in range(NUM_PATIENTS)]}
    return fall


def create_test_FALL_empty() -> dict:
    fall = {'IK':                                      '',
            'Entlassender-Standort':                   '',
            'Entgeltbereich':                          '',
            'KH-internes-Kennzeichen':                 '',
            'Versicherten-ID':                         '',
            'Vertragskennzeichen-64b-Modellvorhaben':  '',
            'IK-der-Krankenkasse':                     '',
            'Geburtsjahr':                             '',
            'Geburtsmonat':                            '',
            'Geschlecht':                              '',
            'PLZ':                                     '',
            'Wohnort':                                 '',
            'Aufnahmedatum':                           '',
            'Aufnahmeanlass':                          '',
            'Aufnahmegrund':                           '',
            'Fallzusammenführung':                     '',
            'Fallzusammenführungsgrund':               '',
            'Entlassungsdatum':                        '',
            'Entlassungsgrund':                        '',
            'Alter-in-Tagen-am-Aufnahmetag':           '',
            'Alter-in-Jahren-am-Aufnahmetag':          '',
            'Aufnahmegewicht':                         '',
            'Patientennummer':                         '',
            'Interkurrente-Dialysen':                  '',
            'Beatmungsstunden':                        '',
            'Behandlungsbeginn-vorstationär':          '',
            'Behandlungstage-vorstationär':            '',
            'Behandlungsende-nachstationär':           '',
            'Behandlungstage-nachstationär':           '',
            'IK-Verlegungs-KH':                        '',
            'Belegungstage-in-anderem-Entgeltbereich': '',
            'Beurlaubungstage-PSY':                    '',
            'Kennung-Besonderer-Fall-Modellvorhaben':  '',
            'Verweildauer-Intensiv':                   ''}
    return fall


def create_test_FALL_missing_cols() -> dict:
    fall = create_test_FALL_max()
    del fall['Aufnahmedatum']
    return fall


def create_test_FALL_no_optional_cols() -> dict:
    fall = create_test_FALL_max()
    del fall['IK']
    del fall['Entlassender-Standort']
    del fall['Entgeltbereich']
    del fall['Versicherten-ID']
    del fall['Vertragskennzeichen-64b-Modellvorhaben']
    del fall['Geburtsmonat']
    del fall['Wohnort']
    del fall['Alter-in-Tagen-am-Aufnahmetag']
    del fall['Alter-in-Jahren-am-Aufnahmetag']
    del fall['Aufnahmegewicht']
    del fall['Patientennummer']
    del fall['Interkurrente-Dialysen']
    del fall['IK-Verlegungs-KH']
    del fall['Belegungstage-in-anderem-Entgeltbereich']
    del fall['Beurlaubungstage-PSY']
    del fall['Kennung-Besonderer-Fall-Modellvorhaben']
    return fall


def create_test_FALL_missing_leading_zeros_in_plz_and_aufnahmegrund() -> dict:
    fall = create_test_FALL_max()
    fall['PLZ'] = [int(str(x)[1:]) for x in fall['PLZ']]
    fall['Aufnahmegrund'] = [x[1:] for x in fall['Aufnahmegrund']]
    return fall


def create_test_FALL_empty_internal_id() -> dict:
    fall = create_test_FALL_max()
    fall['KH-internes-Kennzeichen'] = ['' for x in fall['KH-internes-Kennzeichen']]
    return fall


def create_test_FAB_max() -> dict:
    fab = {'IK':                            ['261700001' for _ in range(NUM_FACTS)],
           'Entlassender-Standort':         ['770001000' for _ in range(NUM_FACTS)],
           'Entgeltbereich':                random.choices(['DRG', 'PSY'], weights=[0.66, 0.3], k=NUM_FACTS),
           'KH-internes-Kennzeichen':       np.random.randint(1000, 4000, NUM_FACTS),
           'Standortnummer-Behandlungsort': ['770001000' for _ in range(NUM_FACTS)],
           'Fachabteilung':                 [random.choice(['HA', 'BA', 'BE']) + str(np.random.randint(1000, 9999)) for _ in range(NUM_FACTS)],
           'FAB-Aufnahmedatum':             [generate_random_date('2020') for _ in range(NUM_FACTS)],
           'FAB-Entlassungsdatum':          [generate_random_date('2021') for _ in range(NUM_FACTS)],
           'Kennung-Intensivbett':          random.choices(['J', 'N'], k=NUM_FACTS)}
    return fab


def create_test_FAB_empty() -> dict:
    fab = {'IK':                            '',
           'Entlassender-Standort':         '',
           'Entgeltbereich':                '',
           'KH-internes-Kennzeichen':       '',
           'Standortnummer-Behandlungsort': '',
           'Fachabteilung':                 '',
           'FAB-Aufnahmedatum':             '',
           'FAB-Entlassungsdatum':          '',
           'Kennung-Intensivbett':          ''}
    return fab


def create_test_FAB_alt() -> dict:
    fab = {'IK':                            '',
           'Entlassender-Standort':         '',
           'Entgeltbereich':                '',
           'KH-internes-Kennzeichen':       '',
           'Standortnummer-Behandlungsort': '',
           'FAB':                           '',
           'FAB-Aufnahmedatum':             '',
           'FAB-Entlassungsdatum':          '',
           'Kennung-Intensivbett':          ''}
    return fab


def create_test_FAB_missing_cols() -> dict:
    fab = create_test_FAB_max()
    del fab['FAB-Aufnahmedatum']
    return fab


def create_test_FAB_no_optional_cols() -> dict:
    fab = create_test_FAB_max()
    del fab['IK']
    del fab['Entlassender-Standort']
    del fab['Entgeltbereich']
    del fab['Standortnummer-Behandlungsort']
    return fab


def create_test_ICD_max() -> dict:
    icd = {'IK':                      ['261700001' for _ in range(NUM_FACTS)],
           'Entlassender-Standort':   ['770001000' for _ in range(NUM_FACTS)],
           'Entgeltbereich':          random.choices(['DRG', 'PSY'], weights=[0.66, 0.3], k=NUM_FACTS),
           'KH-internes-Kennzeichen': np.random.randint(1000, 4000, NUM_FACTS),
           'Diagnoseart':             random.choices(['HD', 'ND'], k=NUM_FACTS),
           'ICD-Version':             ['2019' for _ in range(NUM_FACTS)],
           'ICD-Kode':                random.choices(CODES_ICD, k=NUM_FACTS),
           'Lokalisation':            random.choices(['R', 'L', 'B', ''], k=NUM_FACTS),
           'Diagnosensicherheit':     random.choices(['A', 'V', 'Z', 'G', ''], k=NUM_FACTS),
           'Sekundär-Kode':           random.choices(CODES_ICD, k=NUM_FACTS),
           'Lokalisation.1':          random.choices(['R', 'L', 'B', ''], k=NUM_FACTS),
           'Diagnosensicherheit.1':   random.choices(['A', 'V', 'Z', 'G', ''], k=NUM_FACTS)}
    return icd


def create_test_ICD_empty() -> dict:
    icd = {'IK':                           '',
           'Entlassender-Standort':        '',
           'Entgeltbereich':               '',
           'KH-internes-Kennzeichen':      '',
           'Diagnoseart':                  '',
           'ICD-Version':                  '',
           'ICD-Kode':                     '',
           'Lokalisation':                 '',
           'Diagnosensicherheit':          '',
           'Sekundär-Kode':                '',
           'Sekundär-Lokalisation':        '',
           'Sekundär-Diagnosensicherheit': ''}
    return icd


def create_test_ICD_error() -> dict:
    icd = {'IK':                      '',
           'Entlassender-Standort':   '',
           'Entgeltbereich':          '',
           'KH-internes-Kennzeichen': '',
           'Diagnoseart':             '',
           'ICD-Version':             '',
           'ICD-Kode':                '',
           'Lokalisation':            '',
           'Diagnosensicherheit':     '',
           'Sekundär-Kode':           '',
           'Lokalisation.1':          '',
           'Lokalisation.2':          ''}
    return icd


def create_test_ICD_missing_cols() -> dict:
    icd = create_test_ICD_max()
    del icd['ICD-Kode']
    return icd


def create_test_ICD_no_optional_cols() -> dict:
    icd = create_test_ICD_max()
    del icd['IK']
    del icd['Entlassender-Standort']
    del icd['Entgeltbereich']
    return icd


def create_test_ICD_no_sek() -> dict:
    icd = create_test_ICD_max()
    del icd['Sekundär-Kode']
    del icd['Lokalisation.1']
    del icd['Diagnosensicherheit.1']
    return icd


def create_test_ICD_with_sek() -> dict:
    icd = create_test_ICD_max()
    del icd['Lokalisation.1']
    icd['Sekundär-Lokalisation'] = random.choices(['R', 'L', 'B', ''], k=NUM_FACTS)
    del icd['Diagnosensicherheit.1']
    icd['Sekundär-Diagnosensicherheit'] = random.choices(['A', 'V', 'Z', 'G', ''], k=NUM_FACTS)
    return icd


def create_test_OPS_max() -> dict:
    ops = {'IK':                      ['261700001' for _ in range(NUM_FACTS)],
           'Entlassender-Standort':   ['770001000' for _ in range(NUM_FACTS)],
           'Entgeltbereich':          random.choices(['DRG', 'PSY'], weights=[0.66, 0.3], k=NUM_FACTS),
           'KH-internes-Kennzeichen': np.random.randint(1000, 4000, NUM_FACTS),
           'OPS-Version':             ['2019' for _ in range(NUM_FACTS)],
           'OPS-Kode':                random.choices(CODES_OPS, k=NUM_FACTS),
           'Lokalisation':            random.choices(['R', 'L', 'B', ''], k=NUM_FACTS),
           'OPS-Datum':               [generate_random_date('2020') for _ in range(NUM_FACTS)],
           'Belegoperateur':          ['N' for _ in range(NUM_FACTS)],
           'Beleganästhesist':        ['N' for _ in range(NUM_FACTS)],
           'Beleghebamme':            ['N' for _ in range(NUM_FACTS)]}
    return ops


def create_test_OPS_empty() -> dict:
    ops = {'IK':                      '',
           'Entlassender-Standort':   '',
           'Entgeltbereich':          '',
           'KH-internes-Kennzeichen': '',
           'OPS-Version':             '',
           'OPS-Kode':                '',
           'Lokalisation':            '',
           'OPS-Datum':               '',
           'Belegoperateur':          '',
           'Beleganästhesist':        '',
           'Beleghebamme':            ''}
    return ops


def create_test_OPS_missing_cols() -> dict:
    ops = create_test_OPS_max()
    del ops['OPS-Kode']
    return ops


def create_test_OPS_no_optional_cols() -> dict:
    ops = create_test_OPS_max()
    del ops['IK']
    del ops['Entlassender-Standort']
    del ops['Entgeltbereich']
    del ops['Belegoperateur']
    del ops['Beleganästhesist']
    del ops['Beleghebamme']
    return ops


def add_missing_values(dict_csv: dict, column: str, index: int) -> dict:
    indeces = np.where(np.asarray(dict_csv['KH-internes-Kennzeichen']) == index)
    for i in indeces[0].tolist():
        dict_csv['KH-internes-Kennzeichen'][i] = np.random.randint(10000, 99999)
    dict_csv['KH-internes-Kennzeichen'][index] = index
    dict_csv[column][index] = 'MISSING'
    return dict_csv


def create_test_FALL_conversion() -> dict:
    fall = {'KH-internes-Kennzeichen':        ['4000', '4001', '4002', '4003', '4004', '4005', '4006'],
            'IK-der-Krankenkasse':            ['161556856', '', '', '', '', '', ''],
            'Geburtsjahr':                    ['2000', '', '', '', '', '', ''],
            'Geschlecht':                     ['m', '', '', '', '', '', ''],
            'PLZ':                            ['12345', '', '', '', '', '', ''],
            'Aufnahmedatum':                  ['202001010000' for _ in range(8)],
            'Aufnahmeanlass':                 ['N' for _ in range(8)],
            'Aufnahmegrund':                  ['1010' for _ in range(8)],
            'Fallzusammenführung':            ['J', '', 'N', 'J', '', '', ''],
            'Fallzusammenführungsgrund':      ['OG', '', 'OG', '', '', '', ''],
            'Entlassungsdatum':               ['202101010000', '', '', '', '202101010000', '', ''],
            'Entlassungsgrund':               ['179', '', '', '', '', '179', ''],
            'Beatmungsstunden':               ['500,50', '', '', '', '', '', ''],
            'Behandlungsbeginn-vorstationär': ['20190101', '', '', '', '', '', '20190101'],
            'Behandlungstage-vorstationär':   ['365', '', '', '', '', '', ''],
            'Behandlungsende-nachstationär':  ['20220101', '', '', '', '', '', '20220101'],
            'Behandlungstage-nachstationär':  ['365', '', '', '', '', '', ''],
            'Verweildauer-Intensiv':          ['100,50', '', '', '', '', '', '']}
    return fall


def create_test_FAB_conversion() -> dict:
    fab = {'KH-internes-Kennzeichen': ['4000', '4000', '4001', '4001', '4001'],
           'Fachabteilung':           ['HA0001', 'BE0001', 'HA0001', 'BE0001', 'BE0002'],
           'FAB-Aufnahmedatum':       ['202001010000', '202201010000', '202001010000', '202201010000', '202301010000'],
           'FAB-Entlassungsdatum':    ['202101010000', '', '202101010000', '', ''],
           'Kennung-Intensivbett':    ['J', '', 'J', 'N', 'N']}
    return fab


def create_test_ICD_conversion() -> dict:
    icd = {'KH-internes-Kennzeichen': ['4000', '4000', '4000', '4000', '4001', '4001', '4001'],
           'Diagnoseart':             ['HD', 'ND', 'ND', 'ND', 'HD', 'HD', 'HD'],
           'ICD-Version':             ['2019' for _ in range(8)],
           'ICD-Kode':                ['F2424', 'G25.25', 'J90', 'J21.', 'V97.33XD', 'V0001XD', 'Y93D'],
           'Lokalisation':            ['L', '', 'R', '', '', '', ''],
           'Diagnosensicherheit':     ['A', 'Z', '', '', '', '', ''],
           'Sekundär-Kode':           ['', '', '', '', 'A22.22', 'B11.11', 'C3333'],
           'Lokalisation.1':          ['', '', '', '', 'B', '', 'L'],
           'Diagnosensicherheit.1':   ['', '', '', '', 'A', 'Z', '']}
    return icd


def create_test_OPS_conversion() -> dict:
    ops = {'KH-internes-Kennzeichen': ['4000', '4000', '4000', '4000', '4001', '4001', '4001'],
           'OPS-Version':             ['2019' for _ in range(8)],
           'OPS-Kode':                ['964922', '9-64922', '9649.22', '9-649.22', '1-5020', '1-501', '1051'],
           'Lokalisation':            ['B', 'L', 'R', '', '', '', ''],
           'OPS-Datum':               ['202001010000' for _ in range(8)]}
    return ops


def clean_up():
    csv_files = [item for item in os.listdir() if item.endswith('.csv')]
    for csv_file in csv_files:
        os.remove(csv_file)


if __name__ == '__main__':
    fall = create_test_FALL_max()
    fall = add_missing_values(fall, 'Aufnahmedatum', 1021)
    fall = add_missing_values(fall, 'Aufnahmegrund', 1022)
    fall = add_missing_values(fall, 'Aufnahmeanlass', 1023)
    save_test_data_as_csv_to_local_folder('FALL.csv', fall)
    save_test_data_as_csv_to_local_folder('FALL_empty.csv', create_test_FALL_empty())
    save_test_data_as_csv_to_local_folder('FALL_missing_cols.csv', create_test_FALL_missing_cols())
    save_test_data_as_csv_to_local_folder('FALL_no_optional_cols.csv', create_test_FALL_no_optional_cols())
    fab = create_test_FAB_max()
    fab = add_missing_values(fab, 'Fachabteilung', 1021)
    fab = add_missing_values(fab, 'FAB-Aufnahmedatum', 1022)
    fab = add_missing_values(fab, 'Kennung-Intensivbett', 1023)
    save_test_data_as_csv_to_local_folder('FAB.csv', fab)
    save_test_data_as_csv_to_local_folder('FAB_empty.csv', create_test_FAB_empty())
    save_test_data_as_csv_to_local_folder('FAB_alt.csv', create_test_FAB_alt())
    save_test_data_as_csv_to_local_folder('FAB_missing_cols.csv', create_test_FAB_missing_cols())
    save_test_data_as_csv_to_local_folder('FAB_no_optional_cols.csv', create_test_FAB_no_optional_cols())
    icd = create_test_ICD_max()
    icd = add_missing_values(icd, 'Diagnoseart', 1021)
    icd = add_missing_values(icd, 'ICD-Version', 1022)
    icd = add_missing_values(icd, 'ICD-Kode', 1023)
    save_test_data_as_csv_to_local_folder('ICD.csv', icd)
    save_test_data_as_csv_to_local_folder('ICD_empty.csv', create_test_ICD_empty())
    save_test_data_as_csv_to_local_folder('ICD_error.csv', create_test_ICD_error())
    save_test_data_as_csv_to_local_folder('ICD_missing_cols.csv', create_test_ICD_missing_cols())
    save_test_data_as_csv_to_local_folder('ICD_no_optional_cols.csv', create_test_ICD_no_optional_cols())
    save_test_data_as_csv_to_local_folder('ICD_no_sek.csv', create_test_ICD_no_sek())
    save_test_data_as_csv_to_local_folder('ICD_with_sek.csv', create_test_ICD_with_sek())
    ops = create_test_OPS_max()
    ops = add_missing_values(ops, 'OPS-Version', 1021)
    ops = add_missing_values(ops, 'OPS-Kode', 1022)
    ops = add_missing_values(ops, 'OPS-Datum', 1023)
    save_test_data_as_csv_to_local_folder('OPS.csv', ops)
    save_test_data_as_csv_to_local_folder('OPS_empty.csv', create_test_OPS_empty())
    save_test_data_as_csv_to_local_folder('OPS_missing_cols.csv', create_test_OPS_missing_cols())
    save_test_data_as_csv_to_local_folder('OPS_no_optional_cols.csv', create_test_OPS_no_optional_cols())

    save_test_data_as_csv_to_local_folder('FALL_conv.csv', create_test_FALL_conversion())
    save_test_data_as_csv_to_local_folder('FAB_conv.csv', create_test_FAB_conversion())
    save_test_data_as_csv_to_local_folder('ICD_conv.csv', create_test_ICD_conversion())
    save_test_data_as_csv_to_local_folder('OPS_conv.csv', create_test_OPS_conversion())

    save_test_data_as_csv_to_local_folder('FALL_missing_zeros.csv', create_test_FALL_missing_leading_zeros_in_plz_and_aufnahmegrund())
    save_test_data_as_csv_to_local_folder('FALL_empty_internal_ids.csv', create_test_FALL_empty_internal_id())

    with ZipFile('p21_verification.zip', 'w') as file:
        file.write('FALL.csv')
        file.write('FALL_empty.csv')
        file.write('FALL_no_optional_cols.csv')
        file.write('FALL_missing_cols.csv')
        file.write('FAB.csv')
        file.write('FAB_empty.csv')
        file.write('FAB_no_optional_cols.csv')
        file.write('FAB_missing_cols.csv')
        file.write('ICD.csv')
        file.write('ICD_empty.csv')
        file.write('ICD_no_optional_cols.csv')
        file.write('ICD_missing_cols.csv')
        file.write('ICD_no_sek.csv')
        file.write('ICD_with_sek.csv')
        file.write('OPS.csv')
        file.write('OPS_empty.csv')
        file.write('OPS_no_optional_cols.csv')
        file.write('OPS_missing_cols.csv')

    with ZipFile('p21_preprocess.zip', 'w') as file:
        file.write('FALL.csv')
        file.write('FAB.csv')
        file.write('FAB_alt.csv')
        file.write('ICD.csv')
        file.write('ICD_error.csv')
        file.write('ICD_no_sek.csv')
        file.write('ICD_with_sek.csv')
        file.write('OPS.csv')
        file.write('FALL_missing_zeros.csv')
        file.write('FALL_empty_internal_ids.csv')

    with ZipFile('p21_conversion.zip', 'w') as file:
        file.write('FALL_conv.csv')
        file.write('FAB_conv.csv')
        file.write('ICD_conv.csv')
        file.write('OPS_conv.csv')

    clean_up()
