from unittest.case import TestCase

from weather_data_combiner import get_site_no, get_section_file


class TestWeatherDataCombiner(TestCase):
    def test_get_site_no(self):
        test_name = '2015 01 19_USGS_08121000_Colorado Rv at Colorado City, TX_Streamflow_1923-2015.csv'

        expected_result = '08121000'
        self.assertEquals(get_site_no(test_name), expected_result)

        test_name = '2015 01 19_USGS_0812100as0_Colorado Rv at Colorado City, TX_Streamflow_1923-2015.csv'
        expected_result = None
        self.assertEquals(get_site_no(test_name), expected_result)

    def test_get_precip_files_by_section_no(self):
        section_no = 1

        test_path = r'D:\GitHub\weather_data_combiner\sample_data\ORANGE(MONTHLY)'

        expected_result = [
            r'D:\GitHub\weather_data_combiner\sample_data\ORANGE(MONTHLY)\CRB_NCDCStations_Section_01_prcp_processed.csv',
            r'D:\GitHub\weather_data_combiner\sample_data\ORANGE(MONTHLY)\CRB_NCDCStations_Section_01_tmax_processed.csv',
            r'D:\GitHub\weather_data_combiner\sample_data\ORANGE(MONTHLY)\CRB_NCDCStations_Section_01_tmin_processed.csv'
        ]

        self.assertEquals(get_section_file(section_no, test_path, 'prcp'), expected_result[0])
        self.assertEquals(get_section_file(section_no, test_path, 'tmax'), expected_result[1])
        self.assertEquals(get_section_file(section_no, test_path, 'tmin'), expected_result[2])
