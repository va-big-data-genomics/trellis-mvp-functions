#!/usr/bin/env python3

import json
import mock
import neo4j
import base64
import pytest

from unittest import TestCase

import main

mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'

# Source: https://towardsdatascience.com/represent-hierarchical-data-in-python-cd36ada5c71a
class TestTaxonomyParser(TestCase):

	example_taxonomy = "flat_taxonomy_data.json"
	label_taxonomy_file = 'label-taxonomy.json'

	def test_example_read_from_json(cls):

		taxonomy_parser = main.TaxonomyParser()
		taxonomy_parser.read_from_json(cls.example_taxonomy)

		taxonomy_parser.find_by_name("Firewall")

	def test_label_read_from_json(cls):

		taxonomy_parser = main.TaxonomyParser()
		taxonomy_parser.read_from_json(cls.label_taxonomy_file)

		taxonomy_parser.find_by_name("PersonalisSequencing")