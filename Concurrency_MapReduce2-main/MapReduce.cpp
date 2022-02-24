/* ---------------------------------------------------------------
Práctica 1.
Código fuente: WordCount.c
Grau Informàtica
48255471E Albert Gine Gonzalez
--------------------------------------------------------------- */
#include "MapReduce.h"
#include "Types.h"

#include <dirent.h>
#include <string.h>
#include <pthread.h>

using namespace std;


pthread_t *threads;
pthread_t *threads_reduce;
long int num_threads;
long int num_reducers;
struct SharedThreads *sharedThreadsData;
struct SharedThreadsReduce *sharedThreadsReduceData;

vector<PtrMap> Mappers;
vector<PtrReduce> Reducers;


// Prototipos funciones
void readMapShuffleMultithreading(struct SharedThreads *stData);
void reduceMultithreading(struct SharedThreadsReduce *strData);

inline void AddMap(PtrMap map) { Mappers.push_back(map); };
inline void AddReduce(PtrReduce reducer) { Reducers.push_back(reducer); };

// Constructor MapReduce: directorio/fichero entrada, directorio salida, función Map, función reduce y número de reducers a utilizar.
MapReduce::MapReduce(char *input, char *output, TMapFunction mapf, TReduceFunction reducef, int nreducers)
{
	MapFunction=mapf;
	ReduceFunction=reducef;
	InputPath = input;
	OutputPath = output;

	DIR *dir;
	struct dirent *entry;
	unsigned char isFile =0x8;

	//Create output files depending on nreducers
	num_reducers = nreducers;
	for(int x=0;x<nreducers;x++)
	{
		char filename[256];

		sprintf(filename, "%s/result.r%d", OutputPath, x+1);
		AddReduce(new TReduce(ReduceFunction, filename));
	}

	//Get num_threads we need to alloc memory
	if ((dir=opendir(input))!=NULL) 
	{
  		while ((entry=readdir(dir))!=NULL) 
		{
			if( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 && entry->d_type == isFile ) 
			{
				string fileName = entry->d_name;
				string filePath = "./Test/" + fileName;
				std::ifstream is (filePath, std::ifstream::binary);
				if(is)
				{
					is.seekg(0, is.end);
					int length = is.tellg();
					is.seekg(0, is.beg);
					
					if(length <= 8388608)
					{
						num_threads += 1;
					}
					else
					{
						if(length % 8388608 == 0)
						{
							num_threads += length / 8388608;
						}
						else
						{
							num_threads += (length / 8388608) + 1;
						}
					}
				}
			}
  		}
		closedir(dir);
	}

	//Alloc memory
	threads = new pthread_t[num_threads];
	threads_reduce = new pthread_t[nreducers];
	sharedThreadsData = new SharedThreads[num_threads];
	sharedThreadsReduceData = new SharedThreadsReduce[nreducers];
}

// Procesa diferentes fases del framework mapreduce: split, map, shuffle/merge, reduce.
TError 
MapReduce::Run()
{
	DIR *dir;
	char *input = InputPath;
	struct dirent *entry;
	unsigned char isFile =0x8;
	char input_path[256];
	long int thread_counter = 0;

	//CONCURRENT CASE (MULTIPLE FILES)
	if ((dir=opendir(input))!=NULL) 
	{
		//Read all the files and directories within directory
		while ((entry=readdir(dir))!=NULL)
		{
			if( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 && entry->d_type == isFile ) 
			{
				sprintf(input_path,"%s/%s",input, entry->d_name);
				//Set thread data to share
				sharedThreadsData[thread_counter].actual_thread = thread_counter;
				sharedThreadsData[thread_counter].input_path = input_path;
				sharedThreadsData[thread_counter].ptrMap = new TMap(MapFunction);
				for(int i=0; i<num_reducers; ++i)
				{
					char fn[256];
					sprintf(fn, "%s/result.r%d", OutputPath, i+1);
					sharedThreadsData[thread_counter].reducers.push_back(new TReduce(ReduceFunction, fn));
				}
				//Create threads calling multithreading methods
				if(pthread_create(&(threads[thread_counter]), NULL, (void *(*) (void *)) readMapShuffleMultithreading, &(sharedThreadsData[thread_counter])) != 0)
				{
					perror("Error creación hilos");
					exit(1);
				}
				++thread_counter;
			}
  		}
		//Finish threads to start the reduce phase
		for(int i=0; i<thread_counter; ++i)
		{
			pthread_join(threads[i], NULL);
		}
		free(threads);
		
		//Join the reducers of threads to common reduce vector
		for(int i=0; i<num_reducers; ++i)
		{
			for(int j=0; j<thread_counter; ++j)
			{
				sharedThreadsReduceData[i].actual_thread = j;
				sharedThreadsReduceData[i].reducers.push_back(sharedThreadsData[j].reducers[i]);
			}
			if(pthread_create(&(threads_reduce[i]), NULL, (void *(*) (void *)) reduceMultithreading, &(sharedThreadsReduceData[i])) != 0)
			{
				perror("Error creación hilos");
				exit(1);
			}
		}
		
		//Finish threads to start the reduce phase
		for(int i=0; i<num_reducers; ++i)
		{
			pthread_join(threads_reduce[i], NULL);
		}
		free(threads_reduce);

  		closedir(dir);
	}
	//SEQUENTIAL CASE (1 FILE)
	else 
	{
		if (Split(InputPath)!=COk)
		{
			error("MapReduce::Run-Error Split");
		}
		if (Map()!=COk)
		{
			error("MapReduce::Run-Error Map");
		}
		if (Suffle()!=COk)
		{
			error("MapReduce::Run-Error Merge");
		}
		if (Reduce()!=COk)
		{
			error("MapReduce::Run-Error Reduce");
		}
	}

	return(COk);
}

// Genera y lee diferentes splits: 1 split por fichero.
// Versión secuencial: asume que un único Map va a procesar todos los splits.
TError 
MapReduce::Split(char *input)
{
	DIR *dir;
	struct dirent *entry;
	unsigned char isFile =0x8;
	char input_path[256];

	PtrMap map = new TMap(MapFunction);
	AddMap(map);

	if ((dir=opendir(input))!=NULL) 
	{
  		/* Read all the files and directories within directory */
  		while ((entry=readdir(dir))!=NULL) 
		{
			if( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 && entry->d_type == isFile ) 
			{
		    	printf ("Processing input file %s\n", entry->d_name);
				sprintf(input_path,"%s/%s",input, entry->d_name);
				map->ReadFileTuples(input_path);
			}
  		}
  		closedir(dir);
	} 
	else 
	{
		if (errno==ENOTDIR)
		{	// Read only a File
			if (map->ReadFileTuples(input)!=COk)
			{
				error("MapReduce::Split - Error could not open file");
				return(CErrorOpenInputDir);
			}	
		}
		else 
		{
			error("MapReduce::Split - Error could not open directory");
			return(CErrorOpenInputDir);
		}
	}

	return(COk);
}

// Ejecuta cada uno de los Maps.
TError 
MapReduce::Map()
{
	for(vector<TMap>::size_type m = 0; m != Mappers.size(); m++) 
	{
		if (debug) printf ("DEBUG::Running Map %d\n", (int)m+1);
		if (Mappers[m]->Run()!=COk)
			error("MapReduce::Map Run error.\n");
	}
	return(COk);
}

// Ordena y junta todas las tuplas de salida de los maps. Utiliza una función de hash como
// función de partición, para distribuir las claves entre los posibles reducers.
// Utiliza un multimap para realizar la ordenación/unión.
TError 
MapReduce::Suffle()
{
	TMapOuputIterator it2;

	for(vector<TMap>::size_type m = 0; m != Mappers.size(); m++) 
	{
		 multimap<string, int> output = Mappers[m]->getOutput();

		// Process all mapper outputs
		for (TMapOuputIterator it1=output.begin(); it1!=output.end(); it1=it2)
		{
			TMapOutputKey key = (*it1).first;
			pair<TMapOuputIterator, TMapOuputIterator> keyRange = output.equal_range(key);

			// Calcular a que reducer le corresponde está clave:
			int r = std::hash<TMapOutputKey>{}(key)%Reducers.size();

			if (debug) printf ("DEBUG::MapReduce::Suffle merge key %s to reduce %d.\n", key.c_str(), r);

			// Añadir todas las tuplas de la clave al reducer correspondiente.
			Reducers[r]->AddInputKeys(keyRange.first, keyRange.second);

			// Eliminar todas las entradas correspondientes a esta clave.
	        //for (it2 = keyRange.first;  it2!=keyRange.second;  ++it2)
	        //   output.erase(it2);
			output.erase(keyRange.first,keyRange.second);
			it2=keyRange.second;
		}	
	}
	return(COk);
}

// Ejecuta cada uno de los Reducers.
TError 
MapReduce::Reduce()
{
	for(vector<TReduce>::size_type m = 0; m != Reducers.size(); m++) 
	{
		if (Reducers[m]->Run()!=COk)
			error("MapReduce::Reduce Run error.\n");
	}
	return(COk);
}

//Multithreading implementation of read, map and shuffle methods
void readMapShuffleMultithreading(struct SharedThreads *stData)
{
	//Read phase
	printf("Thread number %ld, processing input file %s\n", stData->actual_thread, stData->input_path);
	if(stData->ptrMap->ReadFileTuples(stData->input_path) != COk) 
	{
		error("MapReduce::Split Run error.\n");
	}
	//Map phase
	printf("Thread number %ld, executing mapping phase\n", stData->actual_thread);
	if(stData->ptrMap->Run() != COk) 
	{
		error("MapReduce::Map Run error.\n");
	}
	//Shuffle phase
	printf("Thread number %ld, executing shuffle phase\n", stData->actual_thread);
	TMapOuputIterator it2;
	multimap<string, int> output = stData->ptrMap->getOutput();
	//Process all mapper outputs
	for(TMapOuputIterator it1=output.begin(); it1!=output.end(); it1=it2)
	{
		TMapOutputKey key = (*it1).first;
		pair<TMapOuputIterator, TMapOuputIterator> keyRange = output.equal_range(key);
		//Calcular a que reducer le corresponde está clave:
		int r = std::hash<TMapOutputKey>{}(key)%num_reducers;
		if(debug) printf ("DEBUG::MapReduce::Suffle merge key %s to reduce %d.\n", key.c_str(), r);
		//Añadir todas las tuplas de la clave al reducer correspondiente.
		stData->reducers[r]->AddInputKeys(keyRange.first, keyRange.second);
		//Eliminar todas las entradas correspondientes a esta clave.
		output.erase(keyRange.first,keyRange.second);
		it2=keyRange.second;
	}
}

//Multithreading implementation of reduce method
void reduceMultithreading(struct SharedThreadsReduce *strData)
{
	printf("Thread number %ld, executing reduce phase\n", strData->actual_thread);
	for(int i=0; i<strData->reducers.size(); ++i) 
	{
		if(strData->reducers[i]->Run() != COk)
		{
			error("MapReduce::Reduce Run error.\n");
		}
	}
}