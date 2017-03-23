package fr.profi.mzdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.model.AcquisitionMode;
import fr.profi.mzdb.model.BoundingBox;
import fr.profi.mzdb.model.Peak;
import fr.profi.mzdb.model.RunSlice;
import fr.profi.mzdb.model.SpectrumHeader;
import fr.profi.mzdb.model.SpectrumSlice;
import fr.profi.mzdb.util.sqlite.SQLite4JavaTest;

import rx.functions.Action0;
import rx.functions.Action1;
import rx.observers.TestSubscriber;

/**
 * 
 * @author JeT Test MzDBReaderAsync class. It is mainly non regression tests as long as I don't know what is expected reading this sample file...
 */
public class MzDbReaderAsyncTest {

	private static final URL filename_OVEMB150205_12 = MzDbReaderAsyncTest.class
			.getResource("/OVEMB150205_12.raw.0.9.7.mzDB");

	static {
		SQLite4JavaTest.checkSQLite();
	}

	static final HashMap<Integer, Integer> runSliceData = new HashMap();

	static {
		runSliceData.put(2, 158);
		runSliceData.put(3, 158);
		runSliceData.put(4, 158);
		runSliceData.put(5, 158);
		runSliceData.put(6, 158);
		runSliceData.put(7, 158);
		runSliceData.put(8, 158);
		runSliceData.put(9, 158);
		runSliceData.put(10, 158);
		runSliceData.put(11, 158);
		runSliceData.put(12, 158);
		runSliceData.put(13, 158);
		runSliceData.put(14, 158);
		runSliceData.put(15, 158);
		runSliceData.put(16, 158);
		runSliceData.put(17, 158);
		runSliceData.put(18, 158);
		runSliceData.put(19, 158);
		runSliceData.put(20, 158);
		runSliceData.put(21, 158);
		runSliceData.put(22, 158);
		runSliceData.put(23, 158);
		runSliceData.put(24, 158);
		runSliceData.put(25, 158);
		runSliceData.put(26, 158);
		runSliceData.put(27, 158);
		runSliceData.put(28, 158);
		runSliceData.put(29, 158);
		runSliceData.put(30, 158);
		runSliceData.put(31, 158);
		runSliceData.put(32, 158);
		runSliceData.put(33, 158);
		runSliceData.put(34, 158);
		runSliceData.put(35, 158);
		runSliceData.put(36, 158);
		runSliceData.put(37, 158);
		runSliceData.put(38, 158);
		runSliceData.put(39, 158);
		runSliceData.put(40, 158);
		runSliceData.put(41, 158);
		runSliceData.put(42, 158);
		runSliceData.put(43, 158);
		runSliceData.put(44, 158);
		runSliceData.put(45, 158);
		runSliceData.put(46, 158);
		runSliceData.put(47, 158);
		runSliceData.put(48, 158);
		runSliceData.put(49, 158);
		runSliceData.put(50, 158);
		runSliceData.put(51, 158);
		runSliceData.put(52, 158);
		runSliceData.put(53, 158);
		runSliceData.put(54, 158);
		runSliceData.put(55, 158);
		runSliceData.put(56, 158);
		runSliceData.put(57, 158);
		runSliceData.put(58, 158);
		runSliceData.put(59, 158);
		runSliceData.put(60, 158);
		runSliceData.put(61, 158);
		runSliceData.put(62, 158);
		runSliceData.put(63, 158);
		runSliceData.put(64, 158);
		runSliceData.put(65, 158);
		runSliceData.put(66, 158);
		runSliceData.put(67, 158);
		runSliceData.put(68, 158);
		runSliceData.put(69, 158);
		runSliceData.put(70, 158);
		runSliceData.put(71, 158);
		runSliceData.put(72, 158);
		runSliceData.put(73, 158);
		runSliceData.put(74, 158);
		runSliceData.put(75, 158);
		runSliceData.put(76, 158);
		runSliceData.put(77, 158);
		runSliceData.put(78, 158);
		runSliceData.put(79, 158);
		runSliceData.put(80, 158);
		runSliceData.put(81, 158);
		runSliceData.put(82, 158);
		runSliceData.put(83, 158);
		runSliceData.put(84, 158);
		runSliceData.put(85, 158);
		runSliceData.put(86, 158);
		runSliceData.put(87, 158);
		runSliceData.put(88, 158);
		runSliceData.put(89, 158);
		runSliceData.put(90, 158);
		runSliceData.put(91, 158);
		runSliceData.put(92, 158);
		runSliceData.put(93, 158);
		runSliceData.put(94, 158);
		runSliceData.put(95, 158);
		runSliceData.put(96, 158);
		runSliceData.put(97, 158);
		runSliceData.put(98, 158);
		runSliceData.put(99, 158);
		runSliceData.put(100, 158);
		runSliceData.put(101, 158);
		runSliceData.put(102, 158);
		runSliceData.put(103, 158);
		runSliceData.put(104, 158);
		runSliceData.put(105, 158);
		runSliceData.put(106, 158);
		runSliceData.put(107, 158);
		runSliceData.put(108, 158);
		runSliceData.put(109, 158);
		runSliceData.put(110, 150);
		runSliceData.put(111, 158);
		runSliceData.put(112, 158);
		runSliceData.put(113, 158);
		runSliceData.put(114, 158);
		runSliceData.put(115, 158);
		runSliceData.put(116, 158);
		runSliceData.put(117, 158);
		runSliceData.put(118, 152);
		runSliceData.put(119, 158);
		runSliceData.put(120, 158);
		runSliceData.put(121, 149);
		runSliceData.put(122, 149);
		runSliceData.put(123, 158);
		runSliceData.put(124, 150);
		runSliceData.put(125, 158);
		runSliceData.put(126, 150);
		runSliceData.put(127, 158);
		runSliceData.put(128, 158);
		runSliceData.put(129, 158);
		runSliceData.put(130, 151);
		runSliceData.put(131, 158);
		runSliceData.put(132, 149);
		runSliceData.put(133, 152);
		runSliceData.put(134, 158);
		runSliceData.put(135, 158);
		runSliceData.put(136, 145);
		runSliceData.put(137, 158);
		runSliceData.put(138, 158);
		runSliceData.put(139, 128);
		runSliceData.put(140, 152);
		runSliceData.put(141, 145);
		runSliceData.put(142, 152);
		runSliceData.put(143, 149);
		runSliceData.put(144, 150);
		runSliceData.put(145, 133);
		runSliceData.put(146, 158);
		runSliceData.put(147, 158);
		runSliceData.put(148, 158);
		runSliceData.put(149, 158);
		runSliceData.put(150, 158);
		runSliceData.put(151, 158);
		runSliceData.put(152, 158);
		runSliceData.put(153, 151);
		runSliceData.put(154, 158);
		runSliceData.put(155, 158);
		runSliceData.put(156, 158);
		runSliceData.put(157, 158);
		runSliceData.put(158, 152);
		runSliceData.put(159, 145);
		runSliceData.put(160, 151);
		runSliceData.put(161, 158);
	}

	/** check synchronously an SQL request that is asynchronously blocking */
	@Test
	public void getBBoxSync_OVEMB150205_12()
			throws URISyntaxException, ClassNotFoundException, FileNotFoundException, SQLiteException {

		final File file_OVEMB150205_12 = new File(filename_OVEMB150205_12.toURI());
		final int[] bboxDataIds1 = { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
				36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71,
				72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105,
				106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134,
				135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163,
				248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276,
				277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305,
				306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334,
				335, 336, 337, 338, 339, 340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363,
				364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392,
				393, 394, 395, 396, 397, 398, 399, 400, 401, 402, 403, 404, 405, 406, 407, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533, 534, 535, 536,
				537, 538, 539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 553, 554, 555, 556, 557, 558, 559, 560, 561, 562, 563, 564, 565,
				566, 567, 568, 569, 570, 571, 572, 573, 574, 575, 576, 577, 578, 579, 580, 581, 582, 583, 584, 585, 586, 587, 588, 589, 590, 591, 592, 593, 594,
				595, 596, 597, 598, 599, 600, 601, 602, 603, 604, 605, 606, 607, 608, 609, 610, 611, 612, 613, 614, 615, 616, 617, 618, 619, 620, 621, 622, 623,
				624, 625, 626, 627, 628, 629, 630, 631, 632, 633, 634, 635, 636, 637, 638, 639, 640, 641, 642, 643, 644, 645, 646, 647, 648, 649, 650, 651, 652,
				653, 654, 655, 656, 657, 658, 659, 660, 661, 662, 663, 664, 665, 666, 667, 668, 669, 670, 671, 672, 673, 674, 675, 676, 677, 678, 679, 680, 681,
				682, 755, 756, 757, 758, 759, 760, 761, 762, 763, 764, 765, 766, 767, 768, 769, 770, 771, 772, 773, 774, 775, 776, 777, 778, 779, 780, 781, 782,
				783, 784, 785, 786, 787, 788, 789, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 801, 802, 803, 804, 805, 806, 807, 808, 809, 810, 811,
				812, 813, 814, 815, 816, 817, 818, 819, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832, 833, 834, 835, 836, 837, 838, 839, 840,
				841, 842, 843, 844, 845, 846, 847, 848, 849, 850, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 863, 864, 865, 866, 867, 868, 869,
				870, 871, 872, 873, 874, 875, 876, 877, 878, 879, 880, 881, 882, 883, 884, 885, 886, 887, 888, 889, 890, 891, 892, 893, 894, 895, 896, 897, 898,
				899, 900, 901, 902, 903, 904, 905, 906, 907, 908, 909, 910, 911, 912, 913, 914, 984, 985, 986, 987, 988, 989, 990, 991, 992, 993, 994, 995, 996,
				997, 998, 999, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020,
				1021, 1022, 1023, 1024, 1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032, 1033, 1034, 1035, 1036, 1037, 1038, 1039, 1040, 1041, 1042, 1043, 1044,
				1045, 1046, 1047, 1048, 1049, 1050, 1051, 1052, 1053, 1054, 1055, 1056, 1057, 1058, 1059, 1060, 1061, 1062, 1063, 1064, 1065, 1066, 1067, 1068,
				1069, 1070, 1071, 1072, 1073, 1074, 1075, 1076, 1077, 1078, 1079, 1080, 1081, 1082, 1083, 1084, 1085, 1086, 1087, 1088, 1089, 1090, 1091, 1092,
				1093, 1094, 1095, 1096, 1097, 1098, 1099, 1100, 1101, 1102, 1103, 1104, 1105, 1106, 1107, 1108, 1109, 1110, 1111, 1112, 1113, 1114, 1115, 1116,
				1117, 1118, 1119, 1120, 1121, 1122, 1123, 1124, 1125, 1126, 1127, 1128, 1129, 1130, 1131, 1132, 1133, 1134, 1135, 1136, 1137, 1138, 1139, 1140,
				1141, 1142, 1143, 1206, 1207, 1208, 1209, 1210, 1211, 1212, 1213, 1214, 1215, 1216, 1217, 1218, 1219, 1220, 1221, 1222, 1223, 1224, 1225, 1226,
				1227, 1228, 1229, 1230, 1231, 1232, 1233, 1234, 1235, 1236, 1237, 1238, 1239, 1240, 1241, 1242, 1243, 1244, 1245, 1246, 1247, 1248, 1249, 1250,
				1251, 1252, 1253, 1254, 1255, 1256, 1257, 1258, 1259, 1260, 1261, 1262, 1263, 1264, 1265, 1266, 1267, 1268, 1269, 1270, 1271, 1272, 1273, 1274,
				1275, 1276, 1277, 1278, 1279, 1280, 1281, 1282, 1283, 1284, 1285, 1286, 1287, 1288, 1289, 1290, 1291, 1292, 1293, 1294, 1295, 1296, 1297, 1298,
				1299, 1300, 1301, 1302, 1303, 1304, 1305, 1306, 1307, 1308, 1309, 1310, 1311, 1312, 1313, 1314, 1315, 1316, 1317, 1318, 1319, 1320, 1321, 1322,
				1323, 1324, 1325, 1326, 1327, 1328, 1329, 1330, 1331, 1332, 1333, 1334, 1335, 1336, 1337, 1338, 1339, 1340, 1341, 1342, 1343, 1344, 1345, 1346,
				1347, 1348, 1349, 1350, 1351, 1352, 1353, 1354, 1355, 1356, 1357, 1358, 1359, 1360, 1361, 1362, 1363, 1364, 1365, 1415, 1416, 1417, 1418, 1419,
				1420, 1421, 1422, 1423, 1424, 1425, 1426, 1427, 1428, 1429, 1430, 1431, 1432, 1433, 1434, 1435, 1436, 1437, 1438, 1439, 1440, 1441, 1442, 1443,
				1444, 1445, 1446, 1447, 1448, 1449, 1450, 1451, 1452, 1453, 1454, 1455, 1456, 1457, 1458, 1459, 1460, 1461, 1462, 1463, 1464, 1465, 1466, 1467,
				1468, 1469, 1470, 1471, 1472, 1473, 1474, 1475, 1476, 1477, 1478, 1479, 1480, 1481, 1482, 1483, 1484, 1485, 1486, 1487, 1488, 1489, 1490, 1491,
				1492, 1493, 1494, 1495, 1496, 1497, 1498, 1499, 1500, 1501, 1502, 1503, 1504, 1505, 1506, 1507, 1508, 1509, 1510, 1511, 1512, 1513, 1514, 1515,
				1516, 1517, 1518, 1519, 1520, 1521, 1522, 1523, 1524, 1525, 1526, 1527, 1528, 1529, 1530, 1531, 1532, 1533, 1534, 1535, 1536, 1537, 1538, 1539,
				1540, 1541, 1542, 1543, 1544, 1545, 1546, 1547, 1548, 1549, 1550, 1551, 1552, 1553, 1554, 1555, 1556, 1557, 1558, 1559, 1560, 1561, 1562, 1563,
				1564, 1565, 1566, 1567, 1568, 1569, 1570, 1571, 1572, 1573, 1574, 1622, 1623, 1624, 1625, 1626, 1627, 1628, 1629, 1630, 1631, 1632, 1633, 1634,
				1635, 1636, 1637, 1638, 1639, 1640, 1641, 1642, 1643, 1644, 1645, 1646, 1647, 1648, 1649, 1650, 1651, 1652, 1653, 1654, 1655, 1656, 1657, 1658,
				1659, 1660, 1661, 1662, 1663, 1664, 1665, 1666, 1667, 1668, 1669, 1670, 1671, 1672, 1673, 1674, 1675, 1676, 1677, 1678, 1679, 1680, 1681, 1682,
				1683, 1684, 1685, 1686, 1687, 1688, 1689, 1690, 1691, 1692, 1693, 1694, 1695, 1696, 1697, 1698, 1699, 1700, 1701, 1702, 1703, 1704, 1705, 1706,
				1707, 1708, 1709, 1710, 1711, 1712, 1713, 1714, 1715, 1716, 1717, 1718, 1719, 1720, 1721, 1722, 1723, 1724, 1725, 1726, 1727, 1728, 1729, 1730,
				1731, 1732, 1733, 1734, 1735, 1736, 1737, 1738, 1739, 1740, 1741, 1742, 1743, 1744, 1745, 1746, 1747, 1748, 1749, 1750, 1751, 1752, 1753, 1754,
				1755, 1756, 1757, 1758, 1759, 1760, 1761, 1762, 1763, 1764, 1765, 1766, 1767, 1768, 1769, 1770, 1771, 1772, 1773, 1774, 1775, 1776, 1777, 1778,
				1779, 1780, 1781, 1850, 1851, 1852, 1853, 1854, 1855, 1856, 1857, 1858, 1859, 1860, 1861, 1862, 1863, 1864, 1865, 1866, 1867, 1868, 1869, 1870,
				1871, 1872, 1873, 1874, 1875, 1876, 1877, 1878, 1879, 1880, 1881, 1882, 1883, 1884, 1885, 1886, 1887, 1888, 1889, 1890, 1891, 1892, 1893, 1894,
				1895, 1896, 1897, 1898, 1899, 1900, 1901, 1902, 1903, 1904, 1905, 1906, 1907, 1908, 1909, 1910, 1911, 1912, 1913, 1914, 1915, 1916, 1917, 1918,
				1919, 1920, 1921, 1922, 1923, 1924, 1925, 1926, 1927, 1928, 1929, 1930, 1931, 1932, 1933, 1934, 1935, 1936, 1937, 1938, 1939, 1940, 1941, 1942,
				1943, 1944, 1945, 1946, 1947, 1948, 1949, 1950, 1951, 1952, 1953, 1954, 1955, 1956, 1957, 1958, 1959, 1960, 1961, 1962, 1963, 1964, 1965, 1966,
				1967, 1968, 1969, 1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990,
				1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2075, 2076, 2077, 2078, 2079,
				2080, 2081, 2082, 2083, 2084, 2085, 2086, 2087, 2088, 2089, 2090, 2091, 2092, 2093, 2094, 2095, 2096, 2097, 2098, 2099, 2100, 2101, 2102, 2103,
				2104, 2105, 2106, 2107, 2108, 2109, 2110, 2111, 2112, 2113, 2114, 2115, 2116, 2117, 2118, 2119, 2120, 2121, 2122, 2123, 2124, 2125, 2126, 2127,
				2128, 2129, 2130, 2131, 2132, 2133, 2134, 2135, 2136, 2137, 2138, 2139, 2140, 2141, 2142, 2143, 2144, 2145, 2146, 2147, 2148, 2149, 2150, 2151,
				2152, 2153, 2154, 2155, 2156, 2157, 2158, 2159, 2160, 2161, 2162, 2163, 2164, 2165, 2166, 2167, 2168, 2169, 2170, 2171, 2172, 2173, 2174, 2175,
				2176, 2177, 2178, 2179, 2180, 2181, 2182, 2183, 2184, 2185, 2186, 2187, 2188, 2189, 2190, 2191, 2192, 2193, 2194, 2195, 2196, 2197, 2198, 2199,
				2200, 2201, 2202, 2203, 2204, 2205, 2206, 2207, 2208, 2209, 2210, 2211, 2212, 2213, 2214, 2215, 2216, 2217, 2218, 2219, 2220, 2221, 2222, 2223,
				2224, 2225, 2226, 2227, 2228, 2229, 2230, 2231, 2232, 2233, 2234, 2306, 2307, 2308, 2309, 2310, 2311, 2312, 2313, 2314, 2315, 2316, 2317, 2318,
				2319, 2320, 2321, 2322, 2323, 2324, 2325, 2326, 2327, 2328, 2329, 2330, 2331, 2332, 2333, 2334, 2335, 2336, 2337, 2338, 2339, 2340, 2341, 2342,
				2343, 2344, 2345, 2346, 2347, 2348, 2349, 2350, 2351, 2352, 2353, 2354, 2355, 2356, 2357, 2358, 2359, 2360, 2361, 2362, 2363, 2364, 2365, 2366,
				2367, 2368, 2369, 2370, 2371, 2372, 2373, 2374, 2375, 2376, 2377, 2378, 2379, 2380, 2381, 2382, 2383, 2384, 2385, 2386, 2387, 2388, 2389, 2390,
				2391, 2392, 2393, 2394, 2395, 2396, 2397, 2398, 2399, 2400, 2401, 2402, 2403, 2404, 2405, 2406, 2407, 2408, 2409, 2410, 2411, 2412, 2413, 2414,
				2415, 2416, 2417, 2418, 2419, 2420, 2421, 2422, 2423, 2424, 2425, 2426, 2427, 2428, 2429, 2430, 2431, 2432, 2433, 2434, 2435, 2436, 2437, 2438,
				2439, 2440, 2441, 2442, 2443, 2444, 2445, 2446, 2447, 2448, 2449, 2450, 2451, 2452, 2453, 2454, 2455, 2456, 2457, 2458, 2459, 2537, 2538, 2539,
				2540, 2541, 2542, 2543, 2544, 2545, 2546, 2547, 2548, 2549, 2550, 2551, 2552, 2553, 2554, 2555, 2556, 2557, 2558, 2559, 2560, 2561, 2562, 2563,
				2564, 2565, 2566, 2567, 2568, 2569, 2570, 2571, 2572, 2573, 2574, 2575, 2576, 2577, 2578, 2579, 2580, 2581, 2582, 2583, 2584, 2585, 2586, 2587,
				2588, 2589, 2590, 2591, 2592, 2593, 2594, 2595, 2596, 2597, 2598, 2599, 2600, 2601, 2602, 2603, 2604, 2605, 2606, 2607, 2608, 2609, 2610, 2611,
				2612, 2613, 2614, 2615, 2616, 2617, 2618, 2619, 2620, 2621, 2622, 2623, 2624, 2625, 2626, 2627, 2628, 2629, 2630, 2631, 2632, 2633, 2634, 2635,
				2636, 2637, 2638, 2639, 2640, 2641, 2642, 2643, 2644, 2645, 2646, 2647, 2648, 2649, 2650, 2651, 2652, 2653, 2654, 2655, 2656, 2657, 2658, 2659,
				2660, 2661, 2662, 2663, 2664, 2665, 2666, 2667, 2668, 2669, 2670, 2671, 2672, 2673, 2674, 2675, 2676, 2677, 2678, 2679, 2680, 2681, 2682, 2683,
				2684, 2685, 2686, 2687, 2688, 2689, 2690, 2691, 2692, 2775, 2776, 2777, 2778, 2779, 2780, 2781, 2782, 2783, 2784, 2785, 2786, 2787, 2788, 2789,
				2790, 2791, 2792, 2793, 2794, 2795, 2796, 2797, 2798, 2799, 2800, 2801, 2802, 2803, 2804, 2805, 2806, 2807, 2808, 2809, 2810, 2811, 2812, 2813,
				2814, 2815, 2816, 2817, 2818, 2819, 2820, 2821, 2822, 2823, 2824, 2825, 2826, 2827, 2828, 2829, 2830, 2831, 2832, 2833, 2834, 2835, 2836, 2837,
				2838, 2839, 2840, 2841, 2842, 2843, 2844, 2845, 2846, 2847, 2848, 2849, 2850, 2851, 2852, 2853, 2854, 2855, 2856, 2857, 2858, 2859, 2860, 2861,
				2862, 2863, 2864, 2865, 2866, 2867, 2868, 2869, 2870, 2871, 2872, 2873, 2874, 2875, 2876, 2877, 2878, 2879, 2880, 2881, 2882, 2883, 2884, 2885,
				2886, 2887, 2888, 2889, 2890, 2891, 2892, 2893, 2894, 2895, 2896, 2897, 2898, 2899, 2900, 2901, 2902, 2903, 2904, 2905, 2906, 2907, 2908, 2909,
				2910, 2911, 2912, 2913, 2914, 2915, 2916, 2917, 2918, 2919, 2920, 2921, 2922, 2923, 2924, 2925, 3011, 3012, 3013, 3014, 3015, 3016, 3017, 3018,
				3019, 3020, 3021, 3022, 3023, 3024, 3025, 3026, 3027, 3028, 3029, 3030, 3031, 3032, 3033, 3034, 3035, 3036, 3037, 3038, 3039, 3040, 3041, 3042,
				3043, 3044, 3045, 3046, 3047, 3048, 3049, 3050, 3051, 3052, 3053, 3054, 3055, 3056, 3057, 3058, 3059, 3060, 3061, 3062, 3063, 3064, 3065, 3066,
				3067, 3068, 3069, 3070, 3071, 3072, 3073, 3074, 3075, 3076, 3077, 3078, 3079, 3080, 3081, 3082, 3083, 3084, 3085, 3086, 3087, 3088, 3089, 3090,
				3091, 3092, 3093, 3094, 3095, 3096, 3097, 3098, 3099, 3100, 3101, 3102, 3103, 3104, 3105, 3106, 3107, 3108, 3109, 3110, 3111, 3112, 3113, 3114,
				3115, 3116, 3117, 3118, 3119, 3120, 3121, 3122, 3123, 3124, 3125, 3126, 3127, 3128, 3129, 3130, 3131, 3132, 3133, 3134, 3135, 3136, 3137, 3138,
				3139, 3140, 3141, 3142, 3143, 3144, 3145, 3146, 3147, 3148, 3149, 3150, 3151, 3152, 3153, 3154, 3155, 3156, 3157, 3158, 3159, 3160, 3161, 3162,
				3163, 3164, 3165, 3166, 3167, 3253, 3254, 3255, 3256, 3257, 3258, 3259, 3260, 3261, 3262, 3263, 3264, 3265, 3266, 3267, 3268, 3269, 3270, 3271,
				3272, 3273, 3274, 3275, 3276, 3277, 3278, 3279, 3280, 3281, 3282, 3283, 3284, 3285, 3286, 3287, 3288, 3289, 3290, 3291, 3292, 3293, 3294, 3295,
				3296, 3297, 3298, 3299, 3300, 3301, 3302, 3303, 3304, 3305, 3306, 3307, 3308, 3309, 3310, 3311, 3312, 3313, 3314, 3315, 3316, 3317, 3318, 3319,
				3320, 3321, 3322, 3323, 3324, 3325, 3326, 3327, 3328, 3329, 3330, 3331, 3332, 3333, 3334, 3335, 3336, 3337, 3338, 3339, 3340, 3341, 3342, 3343,
				3344, 3345, 3346, 3347, 3348, 3349, 3350, 3351, 3352, 3353, 3354, 3355, 3356, 3357, 3358, 3359, 3360, 3361, 3362, 3363, 3364, 3365, 3366, 3367,
				3368, 3369, 3370, 3371, 3372, 3373, 3374, 3375, 3376, 3377, 3378, 3379, 3380, 3381, 3382, 3383, 3384, 3385, 3386, 3387, 3388, 3389, 3390, 3391,
				3392, 3393, 3394, 3395, 3396, 3397, 3398, 3399, 3400, 3401, 3402, 3403, 3404, 3405 };
		try {
			MzDbReader mzDb = new MzDbReader(file_OVEMB150205_12, true);
			int boundingBoxesCount = mzDb.getBoundingBoxesCount();
			Assert.assertEquals(3406, boundingBoxesCount);
			byte[] bboxData = mzDb.getBoundingBoxData(1);
			Assert.assertEquals(1192, bboxData.length);
			Iterator<BoundingBox> boundingBoxIterator = mzDb.getBoundingBoxIterator(1);
			int index = 0;
			while (boundingBoxIterator.hasNext()) {
				BoundingBox bbox = boundingBoxIterator.next();
				Assert.assertEquals(bboxDataIds1[index], bbox.getId());
				index++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Exception thrown: " + e.getMessage());
		}
	}

	/**
	 * Non regression test date: jul 17th 2015
	 *
	 * @throws URISyntaxException
	 * @throws SQLiteException
	 * @throws FileNotFoundException
	 * @throws ClassNotFoundException
	 */
	@Test
	public void readerAsyncTest_OVEMB150205_12()
			throws URISyntaxException, ClassNotFoundException, FileNotFoundException, SQLiteException {

		final float FLOAT_EPSILON = 1E-3f;
		final double DOUBLE_EPSILON = 1E-5d;

		final File file_OVEMB150205_12 = new File(filename_OVEMB150205_12.toURI());
		final long msSpectrumId = 22;
		final long msmssSpectrumId = 23;
		final float minParentMz = 300;
		final float maxParentMz = 450;
		final int bbId = 100;
		final float parentMz = 475.8723755f;
		final float fragmentMz = 592.7037964f;
		final float fragmentMzTolInDa = 0.01f;
		final float minRt = 0;
		final float maxRt = 5000;

		Action1<Throwable> onError = new Action1<Throwable>() {
			@Override
			public void call(Throwable e) {
				System.out.println("Observable Error Type = " + e.getClass().getSimpleName());
				System.out.println("Observable Error Message= " + e.getMessage());
				Assert.fail("Observable Error");
			}

		};
		Action0 onCompleted0 = new Action0() {
			@Override
			public void call() {
			}

		};
		Action1<Object> onCompleted1 = new Action1<Object>() {
			@Override
			public void call(Object arg0) {

			}

		};

		try {
			// create reader in main thread
			Assert.assertTrue("file does not exist", file_OVEMB150205_12.isFile());
			MzDbAsyncReader mzDb = new MzDbAsyncReader(file_OVEMB150205_12, true);
			Assert.assertNotNull("invalid file", mzDb);

			////////////////////////////// Acquisition mode
			TestSubscriber<AcquisitionMode> acquisitionModeTester = new TestSubscriber<>();
			mzDb.getAcquisitionMode().subscribe(acquisitionModeTester);
			acquisitionModeTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			acquisitionModeTester.assertNoErrors();
			List<AcquisitionMode> acquisitionModeList = acquisitionModeTester.getOnNextEvents();
			Assert.assertNotNull(acquisitionModeList);
			Assert.assertEquals(1, acquisitionModeList.size());
			AcquisitionMode acquisitionMode = acquisitionModeList.get(0);
			Assert.assertEquals(AcquisitionMode.UNKNOWN, acquisitionMode);

			////////////////////////////// Bounding box
			TestSubscriber<byte[]> boundingBoxDataTester = new TestSubscriber<>();
			mzDb.getBoundingBoxData(1).subscribe(boundingBoxDataTester);
			boundingBoxDataTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			boundingBoxDataTester.assertNoErrors();
			List<byte[]> boundingBoxDataList = boundingBoxDataTester.getOnNextEvents();
			Assert.assertNotNull(boundingBoxDataList);
			Assert.assertEquals(1, boundingBoxDataList.size());
			byte[] boundingBoxData = boundingBoxDataList.get(0);
			Assert.assertEquals(1192, boundingBoxData.length);

			////////////////////////////// Bounding box first spectrumId
			TestSubscriber<Long> boundingBoxFirstSpectrumIdTester = new TestSubscriber<>();
			mzDb.getBoundingBoxFirstSpectrumId(1).subscribe(boundingBoxFirstSpectrumIdTester);
			boundingBoxFirstSpectrumIdTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			boundingBoxFirstSpectrumIdTester.assertNoErrors();
			List<Long> boundingBoxFirstSpectrumIdList = boundingBoxFirstSpectrumIdTester.getOnNextEvents();
			Assert.assertNotNull(boundingBoxFirstSpectrumIdList);
			Assert.assertEquals(1, boundingBoxFirstSpectrumIdList.size());
			Long firstSpectrumId = boundingBoxFirstSpectrumIdList.get(0);
			Assert.assertEquals(1, firstSpectrumId.longValue());

			////////////////////////////// Bounding box min Mz/Rt
			TestSubscriber<Float> boundingBoxMinMzTester = new TestSubscriber<>();
			mzDb.getBoundingBoxMinMz(bbId).subscribe(boundingBoxMinMzTester);
			boundingBoxMinMzTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			boundingBoxMinMzTester.assertNoErrors();
			List<Float> boundingBoxMinMzList = boundingBoxMinMzTester.getOnNextEvents();
			Assert.assertNotNull(boundingBoxMinMzList);
			Assert.assertEquals(1, boundingBoxMinMzList.size());
			Float boundingBoxMinMz = boundingBoxMinMzList.get(0);
			Assert.assertEquals(880.0f, boundingBoxMinMz.floatValue(), FLOAT_EPSILON);
			TestSubscriber<Float> boundingBoxMinTimeTester = new TestSubscriber<>();
			mzDb.getBoundingBoxMinTime(bbId).subscribe(boundingBoxMinTimeTester);
			boundingBoxMinTimeTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			boundingBoxMinTimeTester.assertNoErrors();
			List<Float> boundingBoxMinTimeList = boundingBoxMinTimeTester.getOnNextEvents();
			Assert.assertNotNull(boundingBoxMinTimeList);
			Assert.assertEquals(1, boundingBoxMinTimeList.size());
			Float boundingBoxMinTime = boundingBoxMinTimeList.get(0);
			Assert.assertEquals(0.1928f, boundingBoxMinTime.floatValue(), FLOAT_EPSILON);

			////////////////////////////// MS level
			TestSubscriber<Integer> boundingBoxMsLevelTester = new TestSubscriber<>();
			mzDb.getBoundingBoxMsLevel(bbId).subscribe(boundingBoxMsLevelTester);
			boundingBoxMsLevelTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			boundingBoxMsLevelTester.assertNoErrors();
			List<Integer> boundingBoxMsLevelList = boundingBoxMsLevelTester.getOnNextEvents();
			Assert.assertNotNull(boundingBoxMsLevelList);
			Assert.assertEquals(1, boundingBoxMsLevelList.size());
			Integer boundingBoxMsLevel = boundingBoxMsLevelList.get(0);
			Assert.assertEquals(1, boundingBoxMsLevel.intValue(), FLOAT_EPSILON);

			////////////////////////////// Cycle count
			TestSubscriber<Integer> cyclesCountTester = new TestSubscriber<>();
			mzDb.getCyclesCount().subscribe(cyclesCountTester);
			cyclesCountTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			cyclesCountTester.assertNoErrors();
			List<Integer> cyclesCountList = cyclesCountTester.getOnNextEvents();
			Assert.assertNotNull(cyclesCountList);
			Assert.assertEquals(1, cyclesCountList.size());
			Integer cyclesCount = cyclesCountList.get(0);
			Assert.assertEquals(158, cyclesCount.intValue(), FLOAT_EPSILON);

			////////////////////////////// Data encoding Count
			TestSubscriber<Integer> dataEncodingsCountTester = new TestSubscriber<>();
			mzDb.getDataEncodingsCount().subscribe(dataEncodingsCountTester);
			dataEncodingsCountTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			dataEncodingsCountTester.assertNoErrors();
			List<Integer> dataEncodingsCountList = dataEncodingsCountTester.getOnNextEvents();
			Assert.assertNotNull(dataEncodingsCountList);
			Assert.assertEquals(1, dataEncodingsCountList.size());
			Integer dataEncodingsCount = dataEncodingsCountList.get(0);
			Assert.assertEquals(3, dataEncodingsCount.intValue());

			////////////////////////////// Last Time
			TestSubscriber<Float> lastTimeTester = new TestSubscriber<>();
			mzDb.getLastTime().subscribe(lastTimeTester);
			lastTimeTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			lastTimeTester.assertNoErrors();
			List<Float> lastTimeList = lastTimeTester.getOnNextEvents();
			Assert.assertNotNull(lastTimeList);
			Assert.assertEquals(1, lastTimeList.size());
			Float lastTime = boundingBoxMinTimeList.get(0);
			Assert.assertEquals(0.1928f, lastTime.floatValue(), FLOAT_EPSILON);

			////////////////////////////// Run Slices
			TestSubscriber<RunSlice> lcmsRunSliceStreamTester = new TestSubscriber<>();
			mzDb.getLcMsRunSliceStream().subscribe(lcmsRunSliceStreamTester);
			lcmsRunSliceStreamTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			lcmsRunSliceStreamTester.assertNoErrors();
			List<RunSlice> lcmsRunSliceStreamList = lcmsRunSliceStreamTester.getOnNextEvents();
			Assert.assertNotNull(lcmsRunSliceStreamList);
			Assert.assertEquals(160, lcmsRunSliceStreamList.size());
			for (int sliceIndex = 0; sliceIndex < lcmsRunSliceStreamList.size(); sliceIndex++) {
				RunSlice slice = lcmsRunSliceStreamList.get(sliceIndex);
				Assert.assertNotNull(slice);
				Assert.assertNotNull(slice.getHeader());
				Assert.assertTrue(slice.getHeader().getId() >= 2 && slice.getHeader().getId() <= 161);
				Assert.assertTrue(slice.getHeader().getMsLevel() == 1);
				Assert.assertTrue(slice.getHeader().getNumber() == slice.getHeader().getId() - 1);
				Assert.assertTrue(slice.getHeader().getBeginMz() == (slice.getHeader().getNumber() - 1) * 5 + 400);
				Assert.assertTrue(slice.getHeader().getEndMz() == (slice.getHeader().getNumber() - 1) * 5 + 405);
				Assert.assertTrue(slice.getHeader().getRunId() == 1);
				Assert.assertTrue(slice.getData().getId() >= 2 && slice.getData().getId() <= 161);
				Assert.assertTrue(slice.getData().getSpectrumSliceList().length == runSliceData.get(slice.getData().getId()));
			}

			////////////////////////////// Run Slices
			TestSubscriber<RunSlice> lcmsRunSliceStreamTester2 = new TestSubscriber<>();
			mzDb.getLcMsRunSliceStream(minParentMz, maxParentMz).subscribe(lcmsRunSliceStreamTester2);
			lcmsRunSliceStreamTester2.awaitTerminalEvent(1, TimeUnit.MINUTES);
			lcmsRunSliceStreamTester2.assertNoErrors();
			List<RunSlice> lcmsRunSliceStreamList2 = lcmsRunSliceStreamTester2.getOnNextEvents();
			Assert.assertNotNull(lcmsRunSliceStreamList2);
			Assert.assertEquals(11, lcmsRunSliceStreamList2.size());
			for (int sliceIndex = 0; sliceIndex < lcmsRunSliceStreamList2.size(); sliceIndex++) {
				RunSlice slice2 = lcmsRunSliceStreamList2.get(sliceIndex);
				Assert.assertNotNull(slice2);
				Assert.assertNotNull(slice2.getHeader());
				Assert.assertTrue(slice2.getHeader().getId() >= 2 && slice2.getHeader().getId() <= 161);
				Assert.assertTrue(slice2.getHeader().getMsLevel() == 1);
				Assert.assertTrue(slice2.getHeader().getNumber() == slice2.getHeader().getId() - 1);
				Assert.assertTrue(slice2.getHeader().getBeginMz() == (slice2.getHeader().getNumber() - 1) * 5 + 400);
				Assert.assertTrue(slice2.getHeader().getEndMz() == (slice2.getHeader().getNumber() - 1) * 5 + 405);
				Assert.assertTrue(slice2.getHeader().getRunId() == 1);
				Assert.assertTrue(slice2.getData().getId() >= 2 && slice2.getData().getId() <= 161);
				Assert.assertTrue(slice2.getData().getSpectrumSliceList().length == runSliceData.get(slice2.getData().getId()));
			}
			////////////////////////////// Max MS level 
			TestSubscriber<Integer> maxMsLevelTester = new TestSubscriber<>();
			mzDb.getMaxMsLevel().subscribe(maxMsLevelTester);
			maxMsLevelTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			maxMsLevelTester.assertNoErrors();
			List<Integer> maxMsLevelList = maxMsLevelTester.getOnNextEvents();
			Assert.assertNotNull(maxMsLevelList);
			Assert.assertEquals(1, maxMsLevelList.size());
			Integer maxMsLevel = maxMsLevelList.get(0);
			Assert.assertEquals(2, maxMsLevel.intValue());

			////////////////////////////// Model Version
			TestSubscriber<String> modelVersionTester = new TestSubscriber<>();
			mzDb.getModelVersion().subscribe(modelVersionTester);
			modelVersionTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			modelVersionTester.assertNoErrors();
			List<String> modelVersionList = modelVersionTester.getOnNextEvents();
			Assert.assertNotNull(modelVersionList);
			Assert.assertEquals(1, modelVersionList.size());
			String modelVersion = modelVersionList.get(0);
			Assert.assertEquals("0.6", modelVersion);

			////////////////////////////// MS1 Spectra headers 
			TestSubscriber<SpectrumHeader[]> ms1SpectrumHeadersTester = new TestSubscriber<>();
			mzDb.getMs1SpectrumHeaders().subscribe(ms1SpectrumHeadersTester);
			ms1SpectrumHeadersTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			ms1SpectrumHeadersTester.assertNoErrors();
			List<SpectrumHeader[]> ms1SpectrumHeadersList = ms1SpectrumHeadersTester.getOnNextEvents();
			Assert.assertNotNull(ms1SpectrumHeadersList);
			Assert.assertEquals(1, ms1SpectrumHeadersList.size());
			SpectrumHeader[] ms1SpectrumHeaders = ms1SpectrumHeadersList.get(0);
			Assert.assertEquals(158, ms1SpectrumHeaders.length);

			////////////////////////////// MS1 Spectra headers with IDs
			TestSubscriber<Map<Long, SpectrumHeader>> ms1SpectrumHeaderByIdTester = new TestSubscriber<>();
			mzDb.getMs1SpectrumHeaderById().subscribe(ms1SpectrumHeaderByIdTester);
			ms1SpectrumHeaderByIdTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			ms1SpectrumHeaderByIdTester.assertNoErrors();
			List<Map<Long, SpectrumHeader>> ms1SpectrumHeaderByIdList = ms1SpectrumHeaderByIdTester.getOnNextEvents();
			Assert.assertNotNull(ms1SpectrumHeaderByIdList);
			Assert.assertEquals(1, ms1SpectrumHeaderByIdList.size());
			Map<Long, SpectrumHeader> ms1SpectrumHeadersById = ms1SpectrumHeaderByIdList.get(0);
			Assert.assertNotNull(ms1SpectrumHeadersById);
			Assert.assertEquals(158, ms1SpectrumHeadersById.size());

			////////////////////////////// MS2 Spectra headers 
			TestSubscriber<SpectrumHeader[]> ms2SpectrumHeadersTester = new TestSubscriber<>();
			mzDb.getMs2SpectrumHeaders().subscribe(ms2SpectrumHeadersTester);
			ms2SpectrumHeadersTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			ms2SpectrumHeadersTester.assertNoErrors();
			List<SpectrumHeader[]> ms2SpectrumHeadersList = ms2SpectrumHeadersTester.getOnNextEvents();
			Assert.assertNotNull(ms2SpectrumHeadersList);
			Assert.assertEquals(1, ms2SpectrumHeadersList.size());
			SpectrumHeader[] ms2SpectrumHeaders = ms2SpectrumHeadersList.get(0);
			Assert.assertEquals(1035, ms2SpectrumHeaders.length);

			////////////////////////////// MS2 Spectra headers with IDs
			TestSubscriber<Map<Long, SpectrumHeader>> ms2SpectrumHeaderByIdTester = new TestSubscriber<>();
			mzDb.getMs2SpectrumHeaderById().subscribe(ms2SpectrumHeaderByIdTester);
			ms2SpectrumHeaderByIdTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			ms2SpectrumHeaderByIdTester.assertNoErrors();
			List<Map<Long, SpectrumHeader>> ms2SpectrumHeaderByIdList = ms2SpectrumHeaderByIdTester.getOnNextEvents();
			Assert.assertNotNull(ms2SpectrumHeaderByIdList);
			Assert.assertEquals(1, ms2SpectrumHeaderByIdList.size());
			Map<Long, SpectrumHeader> ms2SpectrumHeadersById = ms2SpectrumHeaderByIdList.get(0);
			Assert.assertNotNull(ms2SpectrumHeadersById);
			Assert.assertEquals(1035, ms2SpectrumHeadersById.size());

			////////////////////////////// getMsnPeaksInMzRtRanges
			TestSubscriber<Peak[]> msnPeaksInMzRtRangesTester = new TestSubscriber<>();
			mzDb.getMsnPeaksInMzRtRanges(parentMz, 0, 1000, 0, 1000).subscribe(msnPeaksInMzRtRangesTester);
			msnPeaksInMzRtRangesTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			msnPeaksInMzRtRangesTester.assertNoErrors();
			List<Peak[]> msnPeaksInMzRtRangesList = msnPeaksInMzRtRangesTester.getOnNextEvents();
			Assert.assertNotNull(msnPeaksInMzRtRangesList);
			Assert.assertEquals(1, msnPeaksInMzRtRangesList.size());
			Peak[] msnPeaksInMzRtRanges = msnPeaksInMzRtRangesList.get(0);
			Assert.assertNotNull(msnPeaksInMzRtRanges);
			Assert.assertEquals(0, msnPeaksInMzRtRanges.length);// 0 is normal in DDA mode

			////////////////////////////// getMsnPeaksInMzRtRanges
			TestSubscriber<SpectrumSlice[]> msnSpectrumSlicesTester = new TestSubscriber<>();
			mzDb.getMsnSpectrumSlices(parentMz, 0, 1000, 0, 1000).subscribe(msnSpectrumSlicesTester);
			msnSpectrumSlicesTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			msnSpectrumSlicesTester.assertNoErrors();
			List<SpectrumSlice[]> msnSpectrumSlicesList = msnSpectrumSlicesTester.getOnNextEvents();
			Assert.assertNotNull(msnSpectrumSlicesList);
			Assert.assertEquals(1, msnSpectrumSlicesList.size());
			SpectrumSlice[] msnSpectrumSlices = msnSpectrumSlicesList.get(0);
			Assert.assertNotNull(msnSpectrumSlices);
			Assert.assertEquals(0, msnSpectrumSlices.length);// 0 is normal in DDA mode

			////////////////////////////// getMsXic
			//			TestSubscriber<Peak[]> ms1XicTester = new TestSubscriber<>();
			//			mzDb.getMsXic((double)parentMz, 1., 0.5f, 3f, XicMethod.NEAREST).subscribe(ms1XicTester);
			//			ms1XicTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			//			ms1XicTester.assertNoErrors();
			//			List<Peak[]> ms1XicList = ms1XicTester.getOnNextEvents();
			//			Assert.assertNotNull(ms1XicList);
			//			Assert.assertEquals(1, ms1XicList.size());
			//			Peak[] ms1Xic = ms1XicList.get(0);
			//			Assert.assertNotNull(ms1Xic);
			//			Assert.assertEquals(6, ms1Xic.length);

			////////////////////////////// getMsnPeaksInMzRtRanges
			TestSubscriber<Peak[]> ms2XicTester = new TestSubscriber<>();
			mzDb.getMsnXic((double) parentMz, (double) fragmentMz, 1., 0.5f, 2f, XicMethod.NEAREST).subscribe(ms2XicTester);
			ms2XicTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			ms2XicTester.assertNoErrors();
			List<Peak[]> ms2XicList = ms2XicTester.getOnNextEvents();
			Assert.assertNotNull(ms2XicList);
			Assert.assertEquals(1, ms2XicList.size());
			Peak[] ms2Xic = ms2XicList.get(0);
			Assert.assertNotNull(ms2Xic);
			Assert.assertEquals(0, ms2Xic.length);// 0 is normal in DDA mode

		} catch (Throwable e) {
			e.printStackTrace();
			Assert.fail("Exception " + e.getMessage() + " for "
					+ file_OVEMB150205_12.getAbsolutePath());
		}
	}

	/** this test uses local network connection. It may not pass if network configuration change */
	//@Test
	public void largeNetworkRequest() {

		final String[] filenames = {
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_35.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_131.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_82.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_125.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_29.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_76.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_25.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_72.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_120.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_23.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_70.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_118.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_21.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_68.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_116.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_31.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_78.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_127.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_74.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_27.raw.mzDB",
				"\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_122.raw.mzDB" };

		final float[][][] tolMzArray = {
				{ { 0.0057581617f, 575.81616f }, { 0.0057631754f, 576.3175f }, { 0.0057681887f, 576.81885f }, { 0.005773202f, 577.3202f } },
				{ { 0.0057581635f, 575.81635f }, { 0.005763177f, 576.3177f }, { 0.00576819f, 576.81903f }, { 0.005773204f, 577.3204f } },
				{ { 0.005758162f, 575.8162f }, { 0.0057631754f, 576.31757f }, { 0.005768189f, 576.8189f }, { 0.0057732025f, 577.32025f } },
				{ { 0.005758158f, 575.8158f }, { 0.0057631712f, 576.31714f }, { 0.0057681846f, 576.8185f }, { 0.0057731983f, 577.3198f } },
				{ { 0.005758156f, 575.8156f }, { 0.00576317f, 576.31696f }, { 0.005768183f, 576.81836f }, { 0.005773197f, 577.3197f } },
				{ { 0.0057581607f, 575.8161f }, { 0.0057631745f, 576.31744f }, { 0.005768188f, 576.8188f }, { 0.0057732016f, 577.3201f } },
				{ { 0.0057581495f, 575.81494f }, { 0.0057631633f, 576.31635f }, { 0.0057681766f, 576.8177f }, { 0.0057731904f, 577.31903f } },
				{ { 0.0057581165f, 575.81165f }, { 0.0057631303f, 576.313f }, { 0.0057681436f, 576.81433f }, { 0.005773157f, 577.31573f } },
				{ { 0.0057581565f, 575.8157f }, { 0.00576317f, 576.317f }, { 0.0057681836f, 576.81836f }, { 0.005773197f, 577.3197f } },
				{ { 0.0057581547f, 575.8155f }, { 0.0057631684f, 576.31683f }, { 0.0057681818f, 576.8182f }, { 0.0057731955f, 577.3195f } },
				{ { 0.00575816f, 575.816f }, { 0.0057631736f, 576.3173f }, { 0.005768187f, 576.8187f }, { 0.0057732007f, 577.32007f } },
				{ { 0.0057581146f, 575.81146f }, { 0.005763128f, 576.3128f }, { 0.0057681417f, 576.81415f }, { 0.005773155f, 577.3155f } },
				{ { 0.00575816f, 575.816f }, { 0.0057631736f, 576.3173f }, { 0.005768187f, 576.8187f }, { 0.0057732007f, 577.32007f } },
				{ { 0.00575816f, 575.816f }, { 0.0057631736f, 576.3173f }, { 0.005768187f, 576.8187f }, { 0.0057732007f, 577.32007f } },
				{ { 0.00575816f, 575.816f }, { 0.0057631736f, 576.3173f }, { 0.005768187f, 576.8187f }, { 0.0057732007f, 577.32007f } },
				{ { 0.005758161f, 575.8161f }, { 0.0057631745f, 576.31744f }, { 0.005768188f, 576.8188f }, { 0.0057732016f, 577.3201f } },
				{ { 0.0057581617f, 575.81616f }, { 0.0057631754f, 576.3175f }, { 0.0057681887f, 576.8189f }, { 0.0057732025f, 577.32025f } },
				{ { 0.005758164f, 575.8164f }, { 0.0057631773f, 576.31775f }, { 0.005768191f, 576.8191f }, { 0.0057732044f, 577.32043f } },
				{ { 0.00575816f, 575.816f }, { 0.0057631736f, 576.3173f }, { 0.005768187f, 576.8187f }, { 0.0057732007f, 577.32007f } },
				{ { 0.0057581556f, 575.81555f }, { 0.0057631694f, 576.31696f }, { 0.0057681827f, 576.8183f }, { 0.0057731965f, 577.31964f } },
				{ { 0.0057581626f, 575.8163f }, { 0.005763176f, 576.3176f }, { 0.0057681897f, 576.819f }, { 0.005773203f, 577.3203f } } };

		final int[][] nbResultingXics = { { 1, 1, 1, 1 }, { 1, 1, 1, 1 }, { 1, 1, 1, 1 },
				{ 1, 1, 1, 1 }, { 1, 1, 1, 1 }, { 1, 1, 1, 1 },
				{ 1, 1, 1, 1 }, { 1, 1, 1, 1 }, { 1, 1, 1, 1 },
				{ 1, 1, 1, 1 }, { 1, 1, 1, 1 }, { 1, 1, 1, 1 },
				{ 1, 1, 1, 1 }, { 1, 1, 1, 1 }, { 1, 1, 1, 1 },
				{ 1, 1, 1, 1 }, { 1, 1, 1, 1 }, { 1, 1, 1, 1 },
				{ 1, 1, 1, 1 }, { 1, 1, 1, 1 }, { 1, 1, 1, 1 } };

		final int nbFilenames = 21;
		Assert.assertEquals("test has changed. Update it", nbFilenames, filenames.length);
		Assert.assertEquals("there should be as many tols & mzs values as filenames ", filenames.length, tolMzArray.length);

		System.out.print( "{");
		for (int nFilename = 0; nFilename < nbFilenames; nFilename++) {

			File file = new File(filenames[nFilename]);
			float[][] fileTolMz = tolMzArray[nFilename];
			int[] fileNbResultingXics = nbResultingXics[nFilename];
			Assert.assertEquals("there should be as many tols & mzs values as resulting Xics in " + filenames[nFilename], fileTolMz.length,
				fileNbResultingXics.length);
			int nbXics = fileTolMz.length;
			
			System.out.println();
			System.out.print( "{");
			for (int nXic = 0; nXic < nbXics; nXic++) {
				float mz = fileTolMz[nXic][1];
				float tol = fileTolMz[nXic][0];
				int nbResultingXic = fileNbResultingXics[nXic];

				readXic(file, mz, tol, nbResultingXic);
				System.out.print( ",");
			}
			System.out.print( "},");
			System.out.println();
		}
		System.out.println();
		System.out.print( "}");
	}

	private void readXic(File file, float mz, float tol, int nbXics) {
		TestSubscriber<Peak[]> xicTester = new TestSubscriber<>();
		try {
			// create reader in main thread
			Assert.assertTrue("file does not exist", file.isFile());
			MzDbAsyncReader mzdbReader = new MzDbAsyncReader(file, true);
			Assert.assertNotNull("invalid file", mzdbReader);
			mzdbReader.getMsXic(mz, tol, -1.0f, -1.0f, XicMethod.NEAREST).subscribe(xicTester);
			xicTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			xicTester.assertNoErrors();
			List<Peak[]> result = xicTester.getOnNextEvents();
			Assert.assertNotNull(result);
			System.out.print( result.size());
//			Assert.assertEquals(nbXics, result.size());

		} catch (Throwable e) {
			e.printStackTrace();
			Assert.fail("Exception " + e.getMessage() + " for "
					+ file.getAbsolutePath());
		}
	}

	/** this test uses local network connection. It may not pass if network configuration change */
	//@Test
	public void XicBugTest() {
		final float mzOk = 470.7606f;
		final float rtOk = 55.24f*60f;
		final float mzNok = 709.8413f;
		final float rtNok = 28.33f*60f;
		final float tol = 711f;
//		final String filename = "D:\\LCMS\\raw_files\\Fichiers Gamme 2ug Velos ETD\\OEMMA121101_63.raw.mzdb";
		final String filename = "\\\\TOL-BRANDIR\\mzdb\\Karima Chaoui\\Gamme WP4\\QEKAC141027_35.raw.mzDB";

		File file = new File(filename);

		try {
			Assert.assertTrue("file does not exist", file.isFile());
			MzDbAsyncReader mzdbReader = new MzDbAsyncReader(file, true);

			// create reader in main thread
			TestSubscriber<Peak[]> xicTester = new TestSubscriber<>();

			Assert.assertNotNull("invalid file", mzdbReader);
			mzdbReader.getMsXic(mzOk, tol, rtOk -100, rtOk + 100, XicMethod.NEAREST).subscribe(xicTester);
			xicTester.awaitTerminalEvent(1, TimeUnit.MINUTES);
			xicTester.assertNoErrors();
			List<Peak[]> resultOk = xicTester.getOnNextEvents();
			Iterator<Peak[]> peaksIte = resultOk.iterator();
			while ( peaksIte.hasNext() ) {
				System.out.println("\n\n\nPeakList Ok" );
				Peak[] peaks = peaksIte.next();
				for ( int nPeak = 0; nPeak < peaks.length; nPeak++ ) {
					System.out.println(peaks[nPeak].getMz() + ";" + peaks[nPeak].getIntensity() );
				}
			}

			
			TestSubscriber<Peak[]> xicTester2 = new TestSubscriber<>();
			mzdbReader.getMsXic(mzNok, tol,rtNok -100, rtNok + 100, XicMethod.NEAREST).subscribe(xicTester2);
			xicTester2.awaitTerminalEvent(1, TimeUnit.MINUTES);
			xicTester2.assertNoErrors();
			List<Peak[]> resultNok = xicTester2.getOnNextEvents();
			Assert.assertNotNull(resultNok);
			Iterator<Peak[]> peaksIte2 = resultNok.iterator();
			while ( peaksIte2.hasNext() ) {
				System.out.println("\n\n\nPeakList Nok" );
				Peak[] peaks = peaksIte2.next();
				for ( int nPeak = 0; nPeak < peaks.length; nPeak++ ) {
					System.out.println(peaks[nPeak].getMz() + ";" + peaks[nPeak].getIntensity() );
				}
				
			}

		} catch (Throwable e) {
			e.printStackTrace();
			Assert.fail("Exception " + e.getMessage() + " for "
					+ file.getAbsolutePath());
		}

	}
	
	
	// End tests

}
