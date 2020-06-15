<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Str;
use Illuminate\Support\Facades\Storage;

class IngestFiles extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'ingest-files';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Command description';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }


    public function validateData($data, $data_type) {
        if ($data === ""){
            return false;
        }
        if ($data_type === 'INTEGER') {
            if ($data[0] == '-') {
                return ctype_digit(substr($data, 1));
            }
            return ctype_digit($data);
        }
        else if ($data_type === 'BOOLEAN') {
            return $data === "0" || $data === "1";
        }
        else if ($data_type === 'TEXT') {
            return is_string($data);
        }
        return false;
    }

    public function convertData($data, $data_type) {
        //should validate data before converting
        $value = $data;
        if ($data_type === 'INTEGER') {
            $value = (int) $value;
        }
        else if ($data_type === 'BOOLEAN') {
            $value = (boolean) $value;
        }
        else if ($data_type === 'TEXT') {
            return $value;
        }
        return $value;
    }

    public function validateSchema($schema){
        $has_measure_id = false;
        foreach ($schema as $column_def){
            if ($column_def[0] === "measure_id"){
                $has_measure_id = true;
            }
            if (count($column_def) !== 3){
                return false;
            }
            if (!ctype_digit($column_def[1])) {
                $this->error("Length of column should be integer. Column name: \"" . $column_def[0] . "\" Column Length Value: \"" . $column_def[1] . "\"");
                return false;
            }
            if (!in_array($column_def[2], ['TEXT', 'BOOLEAN', 'INTEGER'])) {
                $this->error("Column Type not in [\"TEXT\", \"BOOLEAN\", \"INTEGER\"] . Column name: \"" . $column_def[0] . "\" Column Type Value: \"" . $column_def[2] . "\"");

                return false;
            }
        }
        if (!$has_measure_id){
            $this->error("Schema requires measure_id field");

            return false;
        }
        return true;

    }

    public function parseLine($line, $start, $length){
        return trim(substr($line, $start, $length));
    }

    public function getSchemaFileNames(){
        $files = Storage::files(('/schemas/'));
        $schema_files = collect(preg_grep('/\.csv/', $files))->map(function($filename) {
            return explode('.csv', basename($filename))[0];
        });
        return $schema_files;
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        $client = new \GuzzleHttp\Client();
        $schema_filenames = $this->getSchemaFileNames();

        foreach ($schema_filenames as $schema_filename){
            $schema = collect(file(storage_path('/app/schemas/' . $schema_filename . '.csv' )))->map(function ($value) {
                return collect(str_getcsv($value))->map(function ($v, $k) {
                    return trim($v);
                });
            });

            if (!$this->validateSchema($schema)){
                $this->error("Schema invalid: $schema_filename.csv");
                continue;
            }
            $column_data_length = 0;
            foreach ($schema as $column_def) {
                $column_data_length += $column_def[1];
            }


            if (file_exists(storage_path("/app/data/" . $schema_filename . ".txt"))){
                $file = fopen(storage_path("/app/data/" . $schema_filename . ".txt"), "r");
                $error = false;
                $line_num = 0;
                while(! feof($file) && !$error) {
                    $line = trim(fgets($file));
                    $line_num++;

                    $qm = [];
                    $start = 0;
                    foreach ($schema as $column_def){
                        if ($column_data_length !== strlen($line)){
                            $error = true;
                            $this->error("Data Column Length not valid on line $line_num");
                            break;
                        }

                        $data = $this->parseLine($line, $start,  $column_def[1]);

                        if ($this->validateData($data, $column_def[2])) {
                            $value = $this->convertData($data, $column_def[2]);
                        }
                        else{
                            $error = true;
                            break;
                        }

                        $qm[$column_def[0]] = $value;

                        $start +=  $column_def[1];
                    };

                    //$response = $client->post("https://2swdepm0wa.execute-api.us-east-1.amazonaws.com/prod/NavaInterview/measures", ["json" => $qm]);
                    //$this->info($response->getBody()->getContents());
                }

                if ($error) {
                    $this->error("Error in  \"$schema_filename.txt\" on line $line_num");
                }

                $this->info("Completed ingesting  \"$schema_filename.txt\" ");

                fclose($file);
            }
            else {
                $this->error("No batching data file found for $schema_filename.csv");
            }
        }
    }
}
