<?php

require_once(dirname(__FILE__) . '/model.php');

class LensCLI extends Router
{
	public function __construct()
	{
		parent::__construct();
		$this->sapi['cli']['__DEFAULT__'] = array('file' => PLATFORM_PATH . 'cli.php', 'class' => 'CliHelp');
		$this->sapi['cli']['indexer'] = array('class' => 'LensIndexer', 'description' => 'Launch the continual indexing process');
		$this->sapi['cli']['create'] = array('class' => 'LensCreate', 'description' => 'Create a new event sink');
		$this->sapi['cli']['event'] = array('class' => 'LensEvent', 'description' => 'Log a new event to a sink');
		$this->sapi['cli']['add-index'] = array('class' => 'LensAddIndex', 'description' => 'Create a new index on a sink');
		$this->sapi['cli']['reindex'] = array('class' => 'LensReindex', 'description' => 'Re-index events in a sink');
		$this->sapi['cli']['add-group'] = array('class' => 'LensAddGroup', 'description' => 'Create a new aggregate on a sink');
		$this->sapi['cli']['sinks'] = array('class' => 'LensListSinks', 'description' => 'List available event sinks');
	}
}

abstract class LensCommandLine extends CommandLine
{
	protected $modelClass = 'Lens';
}

class LensCreate extends LensCommandLine
{
	protected function checkargs(&$args)
	{
		if(!parent::checkargs($args)) return false;
		if(count($args) != 1)
		{
			return $this->error(Error::NO_OBJECT, null, null, 'Usage: lens create NAME');
		}
		return true;
	}
	
	public function main($args)
	{
		if(($sink = $this->model->sinkWithName($args[0])))
		{
			echo "A sink named '" . $sink['name'] . "' already exists ({" . $sink['uuid'] . "})\n";
			return 1;
		}
		if(false === $this->model->sinkNameIsValid($args[0]))
		{
			echo "Sink names must be entirely alphanumeric\n";
			return 1;
		}
		if(!($r = $this->model->createSinkWithName($args[0])))
		{
			echo "The event sink '" . $args[0] . "' could not be created\n";
			return 1;
		}
		echo "Sink created as {" . $r . "}\n";
		return 0;
	}
}

class LensEvent extends LensCommandLine
{
	protected $target;
	protected $data = array();
	
	protected function checkargs(&$args)
	{
		if(!parent::checkargs($args)) return false;
		if(count($args) < 2)
		{
			return $this->error(Error::NO_OBJECT, null, null, 'Usage: lens event TARGET KEY=VALUE...');
		}
		$this->target = $args[0];
		array_shift($args);
		foreach($args as $kv)
		{
			$kv = explode('=', $kv, 2);
			if(count($kv) == 1)
			{
				return $this->error(Error::BAD_REQUEST, null, null, 'Usage: lens event TARGET KEY=VALUE...');		
			}
			$this->data[$kv[0]] = $kv[1];
		}
		return true;
	}
	
	public function main($args)
	{
		print_r($this->data);
		$this->model->addEvent($this->target, $this->data, true);
	}
}

class LensAddIndex extends LensCommandLine
{
	protected $target;
	protected $name;
	protected $type;
	protected $length;

	protected function checkargs(&$args)
	{
		if(!parent::checkargs($args)) return false;
		if(count($args) < 3 || count($args) > 4)
		{
			return $this->error(Error::NO_OBJECT, null, null, 'Usage: lens add-index SINK NAME TYPE [LENGTH]');
		}
		$args[2] = strtoupper($args[2]);
		$this->target = $args[0];
		$this->name = $args[1];
		$this->type = strtoupper($args[2]);
		$this->length = isset($args[3]) ? intval($args[3]) : 0;
		if($this->type == 'TEXT' && !$this->length)
		{
			return $this->error(Error::BAD_REQUEST, null, null, 'A length must be specified for TEXT indexes');
		}		
		return true;
	}
	
	public function main($args)
	{
		if(!($sink = $this->model->sinkWithName($this->target)))
		{
			echo "No sink named '" . $this->target . "' exists\n";
			return 1;
		}
		if(($index = $this->model->indexWithNameForSinkWithUuid($this->name, $sink['uuid'])))
		{
			echo "An index named '" . $index['name'] . "' already exists ({" . $index['uuid'] . "})\n";
			return 1;
		}
		if(!($r = $this->model->createIndexForSinkWithName($this->target, $this->name, $this->type, $this->length)))
		{
			echo "The index '" . $this->name . "' could not be created\n";
			return 1;
		}
		echo "Index created as {" . $r . "}\n";
		return 0;
	}
}

class LensReindex extends LensCommandLine
{
	protected $target;
	
	protected function checkargs(&$args)
	{
		if(!parent::checkargs($args)) return false;
		if(count($args) != 1)
		{
			return $this->error(Error::NO_OBJECT, null, null, 'Usage: lens reindex NAME');
		}
		$this->target = $args[0];
		return true;
	}
	
	public function main($args)
	{
		if(!($sink = $this->model->sinkWithName($this->target)))
		{
			echo "No sink named '" . $this->target . "' exists\n";
			return 1;
		}
		$this->model->indexEventsInSinkWithName($this->target);
		return 0;
	}
}

class LensAddGroup extends LensCommandLine
{
	protected $target;
	protected $groupName;
	protected $parent;
	protected $fields;
	
	protected function checkargs(&$args)
	{
		if(!parent::checkargs($args)) return false;
		if(count($args) != 3 && count($args) != 4)
		{
			return $this->error(Error::NO_OBJECT, null, null, 'Usage: lens add-group SINK-NAME GROUP-NAME FIELD-LIST [PARENT-GROUP]');
		}
		$this->target = $args[0];
		$this->groupName = $args[1];
		$this->fields = explode(',', $args[2]);
		if(isset($args[3]))
		{
			$this->parent = $args[3];
		}
		return true;
	}
	
	public function main($args)
	{
		if(!($sink = $this->model->sinkWithName($this->target)))
		{
			echo "No sink named '" . $this->target . "' exists\n";
			return 1;
		}
		$this->model->createGroupForSinkWithName($this->target, $this->groupName, $this->fields, $this->parent);
		return 0;	
	}
}

class LensIndexer extends LensCommandLine
{
	public function main($args)
	{
		require_once(MODULES_ROOT . 'log/model.php');
		Logger::$stderr = true;
		$logger = Logger::getInstance();
		$logger->log('Lens indexer started', LOG_INFO, 'lens', $this->request);
		$shown = false;
		$tcount = 0;
		while(true)
		{
			$count = 0;
			$list = $this->model->sinkNameList();
			foreach($list as $sink)
			{
				$count += $this->model->indexEventsInSinkWithName($sink, 50);
			}
			if($count)
			{
				$tcount += $count;
				$logger->log("Indexed $tcount event" . ($tcount == 1 ? '' : 's') . "...", LOG_INFO, 'lens', $this->request);
				$shown = false;
				sleep(2);
			}
			else
			{
				if(!$shown)
				{
					$logger->log("All updates completed, going to sleep.", LOG_INFO, 'lens', $this->request);
				}
				$shown = true;
				$tcount = 0;
				sleep(10);
			}
		}
	}
}

class LensListSinks extends LensCommandLine
{
	public function main($args)
	{
		$list = $this->model->sinkNameList();
		if(!$list)
		{
			echo "No event sinks have been defined.\n";
			return 0;
		}		
		echo "Available event sinks:\n";
		foreach($list as $n)
		{
			echo "  " . $n . "\n";
		}
	}
}