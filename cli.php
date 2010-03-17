<?php

require_once(dirname(__FILE__) . '/model.php');

class LensCLI extends Router
{
	public function __construct()
	{
		parent::__construct();
		$this->sapi['cli']['__DEFAULT__'] = array('file' => PLATFORM_PATH . 'cli.php', 'class' => 'CliHelp');
		$this->sapi['cli']['create'] = array('class' => 'LensCreate', 'description' => 'Create a new event sink');
		$this->sapi['cli']['event'] = array('class' => 'LensEvent', 'description' => 'Log a new event to a sink');
		$this->sapi['cli']['add-index'] = array('class' => 'LensAddIndex', 'description' => 'Create a new index on a sink');
		$this->sapi['cli']['reindex'] = array('class' => 'LensReindex', 'description' => 'Re-index events in a sink');
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
		$this->model->addEvent($this->target, $this->data);
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
