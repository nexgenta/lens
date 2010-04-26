<?php

/* Copyright 2006, 2007, 2010 Mo McRoberts
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The names of the author(s) of this software may not be used to endorse
 *    or promote products derived from this software without specific prior
 *    written permission.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, 
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY 
 * AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * AUTHORS OF THIS SOFTWARE BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF 
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING 
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS 
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

uses('module');

class LensModule extends Module
{
	public $latestVersion = 4;
	public $moduleId = 'com.nexgenta.lens';
	
	public static function getInstance($args = null, $className = null, $defaultDbIri = null)
	{
		return Model::getInstance($args, ($className ? $className : 'LensModule'), ($defaultDbIri ? $defaultDbIri : LENS_IRI));
	}

	public function updateSchema($targetVersion)
	{
		if($targetVersion == 1)
		{
			$t = $this->db->schema->tableWithOptions('lens__objects', DBTable::CREATE_ALWAYS);
			$t->columnWithSpec('object_uuid', DBType::UUID, null, DBCol::NOT_NULL, null, 'The UUID of the object we’re relating events to');
			$t->columnWithSpec('object_name', DBType::VARCHAR, 32, DBCol::NOT_NULL, null, 'The name of the object we’re relating objects to' );
			$t->indexWithSpec(null, DBIndex::PRIMARY, 'object_uuid');
			$t->indexWithSpec('object_name', DBIndex::UNIQUE, 'object_name');
			return $t->apply();
		}
		if($targetVersion == 2)
		{
			$t = $this->db->schema->tableWithOptions('lens__groups', DBTable::CREATE_ALWAYS);
			$t->columnWithSpec('group_uuid', DBType::UUID, null, DBCol::NOT_NULL, null, 'The UUID of this grouping');
			$t->columnWithSpec('object_uuid', DBType::UUID, null, DBCol::NOT_NULL, null, 'The UUID of the object sink we’re grouping');
			$t->columnWithSpec('group_name', DBType::VARCHAR, 64, DBCol::NOT_NULL, null, 'The name of the group');
			$t->columnWithSpec('group_parent', DBType::UUID, null, DBCol::NULLS, null, 'The UUID of the parent grouping (if any)');
			$t->columnWithSpec('group_fields', DBType::TEXT, null, DBCol::NOT_NULL, null, 'Comma-separated list of fields that we group on');
			$t->indexWithSpec(null, DBIndex::PRIMARY, 'group_uuid');
			$t->indexWithSpec('object_uuid', DBIndex::INDEX, 'object_uuid');
			$t->indexWithSpec('group_name', DBIndex::INDEX, 'group_name');
			$t->indexWithSpec('group_parent', DBIndex::INDEX, 'group_parent');
			return $t->apply();
		}
		if($targetVersion == 3)
		{
			$t = $this->db->schema->tableWithOptions('lens__indices', DBTable::CREATE_ALWAYS);
			$t->columnWithSpec('index_uuid', DBType::UUID, null, DBCol::NOT_NULL, null, 'The UUID of this index');
			$t->columnWithSpec('object_uuid', DBType::UUID, null, DBCol::NOT_NULL, null, 'The UUID of the object sink we’re indexing');
			$t->columnWithSpec('group_uuid', DBType::UUID, null, DBCol::NULLS, null, 'If an aggregate, the UUID of the grouping the aggregate applies to');
			$t->columnWithSpec('index_name', DBType::VARCHAR, 32, DBCol::NOT_NULL, null, 'The name of the index (alphanumeric only)');
			$t->columnWithSpec('index_type', DBType::VARCHAR, 16, DBCol::NOT_NULL, null, 'The data type of the index');
			$t->columnWithSpec('index_length', DBType::INT, null, DBCol::NULLS, null, 'The length of the index');
			$t->columnWithSpec('index_function', DBType::ENUM, array('SUM','COUNT','AVG'), DBCol::NULLS, null, 'If an aggregate, the function used to aggregate values');
			$t->indexWithSpec(null, DBIndex::PRIMARY, 'index_uuid');
			$t->indexWithSpec('object_uuid', DBIndex::INDEX, 'object_uuid');
			$t->indexWithSpec('group_uuid', DBIndex::INDEX, 'group_uuid');
			$t->indexWithSpec('index_name', DBIndex::INDEX, 'index_name');			
			return $t->apply();
		}
		if($targetVersion == 4)
		{
			$objects = $this->db->rows('SELECT * FROM {lens__objects}');
			foreach($objects as $target)
			{
				$t = $this->db->schema->table('lens_' . $target['object_name']);
				$t->columnWithSpec('_minute', DBType::INT, null, DBCol::NOT_NULL, null, 'The minute at which the event occurred');
				$t->columnWithSpec('_second', DBType::INT, null, DBCol::NOT_NULL, null, 'The second at which the event occurred');
				$t->indexWithSpec('_minute', DBIndex::INDEX, '_minute');
				$t->indexWithSpec('_second', DBIndex::INDEX, '_second');
				$t->apply();
			}
			return true;
		}
	}
}
