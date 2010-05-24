<?php

/* Copyright 2006, 2007, 2010 Mo McRoberts.
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

uses('model', 'uuid', 'dbschema');

if(!defined('LENS_IRI')) define('LENS_IRI', null);

class Lens extends Model
{
	protected $schema;
	protected $sinks;
	protected $indices;
	
	public static function getInstance($args = null, $className = null, $defaultDbIri = null)
	{
		return Model::getInstance($args, ($className ? $className : 'Lens'), ($defaultDbIri ? $defaultDbIri : LENS_IRI));
	}

	public function sinkNameIsValid($name, $verbose = false)
	{
		$name = strtolower(trim($name));
		if(!strlen($name) || !ctype_alnum($name))
		{
			if($verbose)
			{
				trigger_error('Lens::createObjectSink(): name must be alphanumeric and not zero-length', E_USER_NOTICE);		
			}
			return false;
		}
		return $name;
	}
	
	public function sinkWithName($name)
	{
		if(false === ($name = $this->sinkNameIsValid($name, true)))
		{
			return false;
		}
		if(isset($this->sinks[$name]))
		{
			return $this->sinks[$name];
		}
		if(!($row = $this->db->row('SELECT "object_uuid" AS "uuid", "object_name" AS "name" FROM {lens__objects} WHERE "object_name" = ?', $name)))
		{
			return false;
		}
		$this->sinks[$row['uuid']] = $row;
		$this->sinks[$row['name']] = $row;
		return $row;
	}
	
	public function sinkNameList()
	{
		if(!($rows = $this->db->rows('SELECT "object_name" AS "name" FROM {lens__objects}')))
		{
			return array();
		}
		$list = array();
		foreach($rows as $r)
		{
			$list[] = $r['name'];		
		}
		return $list;
	}
	
	public function createSinkWithName($name)
	{
		if(false === ($name = $this->sinkNameIsValid($name, true)))
		{
			return false;
		}
		$uuid = UUID::generate();
		do
		{
			$this->db->begin();
			if($this->sinkWithName($name))
			{
				$this->db->rollback();
				trigger_error('Lens::createObjectSink(): a sink named "' . $name . '" already exists', E_USER_NOTICE);
				return false;
			}
			$this->db->insert('lens__objects', array(
				'object_uuid' => $uuid,
				'object_name' => $name,
			));
		}
		while(!$this->db->commit());
		
		$t = $this->db->schema->tableWithOptions('lens_' . $name, DBTable::CREATE_ALWAYS);
		$t->columnWithSpec('_uuid', DBType::UUID, null, DBCol::NOT_NULL, null, 'Unique identifier for this event');
		$t->columnWithSpec('_timestamp', DBType::DATETIME, null, DBCol::NOT_NULL, null, 'Timestamp of the entry');
		$t->columnWithSpec('_year', DBType::INT, null, DBCol::NOT_NULL|DBCol::UNSIGNED, null, 'Year the entry was created (4-digit year)');
		$t->columnWithSpec('_month', DBType::INT, null, DBCol::NOT_NULL|DBCol::UNSIGNED, null, 'Month the entry was created (1-12)');
		$t->columnWithSpec('_day', DBType::INT, null, DBCol::NOT_NULL|DBCol::UNSIGNED, null, 'Day of the month the entry was created (1-31)');
		$t->columnWithSpec('_weekday', DBType::INT, null, DBCol::NOT_NULL|DBCol::UNSIGNED, null, 'Day of the week the entry was created (0-6; 0 = Sunday)');
		$t->columnWithSpec('_yearweek', DBType::INT, null, DBCol::NOT_NULL|DBCol::UNSIGNED, null, 'ISO-8601:1988 Week of the year the entry was created (1-53)');
		$t->columnWithSpec('_yearday', DBType::INT, null, DBCol::NOT_NULL|DBCol::UNSIGNED, null, 'Day of the year the entry was created (1-366)');
		$t->columnWithSpec('_hour', DBType::INT, null, DBCol::NOT_NULL|DBCol::UNSIGNED, null, 'Hour of the day the entry was created (0-23)');
		$t->columnWithSpec('_minute', DBType::INT, null, DBCol::NOT_NULL|DBCol::UNSIGNED, null, 'Minute at which the event occurred (0-59)');
		$t->columnWithSpec('_second', DBType::INT, null, DBCol::NOT_NULL|DBCol::UNSIGNED, null, 'Second at which the event occurred (0-59)');
		$t->columnWithSpec('_dirty', DBType::BOOL, null, DBCol::NOT_NULL, null, 'If "Y", this entry must be re-indexed');
		$t->columnWithSpec('_kind', DBType::VARCHAR, 32, DBCol::NULLS, null, 'Optional event type');
		$t->columnWithSpec('_data', DBType::TEXT, null, DBCol::NULLS, null, 'Event data');
		$t->indexWithSpec('_year', DBIndex::INDEX, '_year');
		$t->indexWithSpec('_month', DBIndex::INDEX, '_month');
		$t->indexWithSpec('_day', DBIndex::INDEX, '_day');
		$t->indexWithSpec('_weekday', DBIndex::INDEX, '_weekday');
		$t->indexWithSpec('_yearweek', DBIndex::INDEX, '_yearweek');
		$t->indexWithSpec('_yearday', DBIndex::INDEX, '_yearday');
		$t->indexWithSpec('_hour', DBIndex::INDEX, '_hour');
		$t->indexWithSpec('_minute', DBIndex::INDEX, '_minute');
		$t->indexWithSpec('_second', DBIndex::INDEX, '_second');
		$t->indexWithSpec('_dirty', DBIndex::INDEX, '_dirty');
		$t->indexWithSpec('_kind', DBIndex::INDEX, '_kind');
		if(!$t->apply())
		{
			$this->db->exec('DELETE FROM {lens__objects} WHERE "object_uuid" = ?', $uuid);
			return false;
		}
		return $uuid;
	}
	
	public function createIndexForSinkWithName($targ, $name, $type, $length)
	{
		if(!($target = $this->sinkWithName($targ)))
		{
			trigger_error('Lens::createIndexForSinkWithName(): The sink "' . $targ . '" does not exist', E_USER_NOTICE);
			return false;
		}
		
		$length = intval($length);
		$flags = DBCol::NULLS;
		switch($type)
		{
			case 'TEXT':
				if($length < 1 || $length > 255)
				{
					trigger_error('Lens::createIndexForSinkWithName(): Invalid length for a TEXT index (must be between 1 and 255)', E_USER_NOTICE);
					return false;
				}
				$native = DBType::VARCHAR;
				break;
			case 'INT':
				$length = null;
				$native = DBType::INT;
				$flags |= DBCol::BIG;
				break;
			default:
				trigger_error('Lens::createIndexForSinkWithName(): Unsupported index type "'.  $type . '" (must be one of TEXT, INT)', E_USER_NOTICE);
				return false;
		}
		$uuid = UUID::generate();
		do
		{
			$this->db->begin();
			if(($this->indexWithNameForSinkWithUuid($name, $target['uuid'])))
			{
				$this->db->rollback();
				trigger_error('Lens::createIndexForSinkWithName(): an index named "' . $name . '" already exists on the sink "' . $target['name'] . '" ({' . $target['uuid'] . '})', E_USER_NOTICE);
				return false;
			}
			$this->db->insert('lens__indices', array(
				'index_uuid' => $uuid,
				'object_uuid' => $target['uuid'],
				'group_uuid' => null,
				'index_name' => $name,
				'index_type' => $type,
				'index_length' => $length,
				'index_function' => null,
			));
		}
		while(!$this->db->commit());
		$t = $this->db->schema->tableWithOptions('lens_' . $target['name'], DBTable::CREATE_NEVER);
		$t->columnWithSpec($name, $native, $length, $flags, null, 'Index for the event value ' . $name);
		$t->indexWithSpec($name, DBIndex::INDEX, $name);
		if(!$t->apply())
		{
			$this->db->exec('DELETE FROM {lens__indices} WHERE "index_uuid" = ?', $uuid);
			return false;
		}
		unset($this->indices[$target['uuid']]);
		$this->db->exec('UPDATE {lens_' . $target['name'] . '} SET "_dirty" = ?', 'Y');
		return $uuid;
	}
	
	public function groupWithNameForSinkWithUuid($name, $target)
	{
		if(($row = $this->db->row('SELECT "group_uuid" AS "uuid", "group_name" AS "name", "group_parent" AS "parent", "group_fields" AS "fields" FROM {lens__groups} WHERE "object_uuid" = ? AND "group_name" = ?', $target, $name)))
		{
			$row['fields'] = explode(',', $row['fields']);
		}
		return $row;
	}
	
	public function createGroupForSinkWithName($targ, $name, $fields, $parent = null)
	{
		if(!($target = $this->sinkWithName($targ)))
		{
			trigger_error('Lens::createGroupForSinkWithName(): The sink "' . $targ . '" does not exist', E_USER_NOTICE);
			return false;
		}
		if(strlen($parent))
		{
			if(!($group = $this->groupWithNameForSinkWithUuid($parent, $target['uuid'])))
			{
				trigger_error('Lens::createGroupForSinkWithName(): The group "' . $parent . '" does not exist', E_USER_NOTICE);
				return false;
			}
		}
		$fieldList = array();
		foreach($fields as $f)
		{
			if($f == '_year' || $f == '_month' || $f == '_day' || $f == '_weekday' || $f == '_yearweek' || $f == '_yearday' || $f == '_hour' || $f == '_minute' || $f == '_second')
			{
				$fieldList[$f] = array(DBType::INT, null, DBCol::NOT_NULL|DBCol::UNSIGNED, null, 'Value from the grouped ' . $f . ' field', true);
			}
			else
			{
				trigger_error('Lens::createGroupForSinkWithName(): Cannot aggregate on unknown field "' . $f . '"', E_USER_WARNING);
			}
		}
		$uuid = UUID::generate();
		do
		{
			$this->db->begin();
			if(($me = $this->groupWithNameForSinkWithUuid($name, $target['uuid'])))
			{
				$this->db->rollback();
				trigger_error('Lens::createGroupForSinkWithName(): The group "' . $name . '" already exists', E_USER_NOTICE);
				return false;
			}
			$this->db->insert('lens__groups', array(
				'group_uuid' => $uuid,
				'object_uuid' => $target['uuid'],
				'group_name' => $name,
				'group_parent' => strlen($parent) ? $group['uuid'] : null,
				'group_fields' => implode(',', $fields),
			));
		}
		while(!$this->db->commit());
		$t = $this->db->schema->tableWithOptions('lens_' . $target['name'] . '_' . $name, DBTable::CREATE_ALWAYS);
		$t->columnWithSpec('_count', DBType::INT, null, DBCol::NOT_NULL|DBCol::UNSIGNED|DBCol::BIG, 0, 'The number of events in this aggregated collection');
		$t->columnWithSpec('_dirty', DBType::BOOL, null, DBCol::NOT_NULL, null, 'If "Y", this entry must be re-indexed');
		$t->indexWithSpec('_dirty', DBIndex::INDEX, '_dirty');
		foreach($fieldList as $k => $field)
		{
			$t->columnWithSpec($k, $field[0], $field[1], $field[2], $field[3], $field[4]);
			if($field[5])
			{
				$t->indexWithSpec($k, DBIndex::INDEX, $k);
			}
		}
		if(!$t->apply())
		{
			$this->db->exec('DELETE FROM {lens__groups} WHERE "group_uuid" = ?', $uuid);
			return false;
		}
		do
		{
			$this->db->begin();
			if(strlen($parent))
			{
				$this->db->exec('UPDATE {lens_' . $target['name'] . '_' . $group['name'] . '} SET "_dirty" = ?', 'Y');
			}
			else
			{
				$this->db->exec('UPDATE {lens_' . $target['name'] . '} SET "_dirty" = ?' , 'Y');
			}
		}
		while(!$this->db->commit());
		return $uuid;
	}
	
	public function addEvent($name, $data, $lazy = false, $cascade = false)
	{
		if(!($target = $this->sinkWithName($name)))
		{
			return false;
		}	
		$kind = null;
		if(is_array($data) || is_object($data))
		{
			$json = json_encode($data);
			if(isset($data['kind']))
			{
				$kind = $data['kind'];
			}
		}
		else if(!strlen($data))
		{
			$json = json_encode(array());
			$data = array();
		}
		else
		{
			$json = $data;
			$data = json_decode($json, true);
		}
		$now = time();
		$uuid = UUID::generate();
		$fields =  array(
			'_uuid' => $uuid,
			'_timestamp' => gmstrftime('%Y-%m-%d %H:%M:%S', $now),
			'_year' => gmstrftime('%Y', $now),
			'_month' => gmstrftime('%m', $now),
			'_day' => gmstrftime('%d', $now),
			'_weekday' => gmstrftime('%w', $now),
			'_yearweek' => gmstrftime('%V', $now),
			'_yearday' => gmstrftime('%j', $now),
			'_hour' => gmstrftime('%H', $now),			
			'_minute' => gmstrftime('%M', $now),
			'_second' => gmstrftime('%S', $now),
			'_dirty' => 'Y',
			'_kind' => $kind,
			'_data' => $json,
		);
		$this->db->insert('lens_' . $target['name'], $fields);
		if(!$lazy)
		{
			$this->indexEvent($target, $uuid, $now, $data, $fields, $lazy, $cascade);
		}
		return $uuid;
	}
	
	public function indexesForSinkWithUuid($uuid)
	{
		if(isset($this->indices[$uuid]))
		{
			return $this->indices[$uuid];
		}
		if(!($rows = $this->db->rows('SELECT "index_uuid" AS "uuid", "index_name" AS "name", "index_type" AS "type", "index_length" AS "length" FROM {lens__indices} WHERE "object_uuid" = ? AND "group_uuid" IS NULL', $uuid)))
		{
			return array();
		}
		$this->indices[$uuid] = $rows;
		return $rows;
	}

	public function indexWithNameForSinkWithUuid($name, $uuid)
	{
		return $this->db->row('SELECT "index_uuid" AS "uuid", "index_name" AS "name", "index_type" AS "type", "index_length" AS "length" FROM {lens__indices} WHERE "object_uuid" = ? AND "group_uuid" IS NULL AND "index_name" = ?', $uuid, $name);
	}
	
	public function indexEventsInSinkWithName($targ, $limit = 0)
	{
		if(!($target = $this->sinkWithName($targ)))
		{
			trigger_error('Lens::createIndexForSinkWithName(): The sink "' . $targ . '" does not exist', E_USER_NOTICE);
			return false;
		}		
		for($c = 0; !$limit || $c < $limit; $c++)
		{
			$event = $this->db->row('SELECT "_uuid", "_timestamp", "_data" FROM {lens_' . $target['name'] . '} WHERE "_dirty" = ?', 'Y');
			if(!$event) break;
			$timestamp = strtotime($event['_timestamp']);
			$data = json_decode($event['_data'], true);
			$this->indexEvent($target, $event['_uuid'], $timestamp, $data, $event, false, false);
		}
		return $c;
	}
	
	protected function indexEvent($target, $uuid, $timestamp, $data, $event, $lazy = true, $cascade = true)
	{
		$indexes = $this->indexesForSinkWithUuid($target['uuid']);
		
		$values = array('"_dirty" = ?');
		$args = array('N');
				
		foreach($indexes as $index)
		{
			$values[] = '"' . $index['name'] . '" = ?';
			if(isset($data[$index['name']]))
			{
				$args[] = $data[$index['name']];
				$event[$index['name']] = $data[$index['name']];
			}
			else
			{
				$args[] = null;
				$event[$index['name']] = null;
			}
		}
		$event['_year'] = $year = gmstrftime('%Y', $timestamp);
		$event['_month'] = gmstrftime('%m', $timestamp);
		$event['_day'] = gmstrftime('%d', $timestamp);
		$event['_weekday'] = gmstrftime('%w', $timestamp);
		$event['_yearweek'] = gmstrftime('%V', $timestamp);
		$event['_yearday'] = $yearday = gmstrftime('%j', $timestamp);
		$event['_hour'] = $hour = gmstrftime('%H', $timestamp);
		$event['_minute'] = gmstrftime('%M', $timestamp);
		$event['_second'] = gmstrftime('%S', $timestamp);

		$criteria = array('"_year" = ?');
		$args[] = $year;

		$criteria[] = '"_yearday" = ?';
		$args[] = $yearday;
		
		$criteria[] = '"_hour" = ?';
		$args[] = $hour;

		$criteria[] = '"_uuid" = ?';
		$args[] = $uuid;

		if(count($values))
		{
			$q = 'UPDATE {lens_' . $target['name'] . '} SET ' . implode(', ', $values) . ' WHERE ' . implode(' AND ', $criteria);
			$this->db->vexec($q, $args);
		}
		$this->performAggregatesForTarget($target, null, $data, $event, $timestamp, $cascade, $lazy);
	}
	
	protected function performAggregatesForTarget($target, $parent, $data, $event, $timestamp, $cascade, $lazy)
	{
		if($lazy)
		{
			$cascade = false;
		}
		if($parent === null)
		{
			$groups = $this->db->rows('SELECT "group_uuid" AS "uuid", "group_name" AS "name", "group_fields" AS "fields" FROM {lens__groups} WHERE "object_uuid" = ? AND "group_parent" IS NULL', $target['uuid']);
		}
		else
		{
			$groups = $this->db->rows('SELECT "group_uuid" AS "uuid", "group_name" AS "name", "group_fields" AS "fields" FROM {lens__groups} WHERE "object_uuid" = ? AND "group_parent" = ?', is_array($parent ? $parent['uuid'] : $parent));
		}
		foreach($groups as $group)
		{
			$fields = explode(',', $group['fields']);
			$values = array();
			foreach($fields as $f)
			{
				if(isset($event[$f]))
				{
					$values[$f] = $event[$f];
				}
				else
				{
					$values[$f] = null;
				}
			}
			$criteria = array();
			foreach($values as $k => $v)
			{
				if($v === null)
				{
					$criteria[] = '"' . $k . '" IS NULL';
				}
				else
				{
					$criteria[] = '"' . $k . '" = ' . $this->db->quote($v);
				}
			}
			$table = 'lens_' . $target['name'] . '_' . $group['name'];
			do
			{
				$this->db->begin();
				if(($row = $this->db->row('SELECT * FROM {' . $table . '} WHERE ' . implode(' AND ', $criteria))))
				{
					$this->db->exec('UPDATE {' . $table . '} SET "_dirty" = ? WHERE ' . implode(' AND ', $criteria), 'Y');
				}
				else
				{
					$values['_dirty'] = 'Y';
					$this->db->insert($table, $values);
				}
			}
			while(!$this->db->commit());
			/* If not lazy, perform aggregate value updates */
			/* If cascade, pass this group onto children */
		}
	}
}
