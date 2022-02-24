#ifndef MAPREDUCE_H_
#define MAPREDUCE_H_

#include "Map.h"
#include "Reduce.h"

#include <functional>
#include <string>

struct SharedThreads {
	long int actual_thread;
	char * input_path;
	PtrMap ptrMap;
	vector<PtrReduce> reducers;
};

struct SharedThreadsReduce {
	long int actual_thread;
	vector<PtrReduce> reducers;
};

class MapReduce 
{
	char *InputPath;
	char *OutputPath;
	TMapFunction MapFunction;
	TReduceFunction ReduceFunction;

	public:
		MapReduce(char * input, char *output, TMapFunction map, TReduceFunction reduce, int nreducers=2);
		TError Run();
		//void SplitMultithreading(struct SharedThreads *stData);
		//void MapMultithreading(struct SharedThreads *stData);
		//void ShuffleMultithreading(struct SharedThreads *stData);
		void ReadMapShuffleMultithreading(struct SharedThreads *stData);
		void ReduceMultithreading(struct SharedThreadsReduce *strData);

	private:
		TError Split(char *input);
		TError Map();
		TError Suffle();
		TError Reduce();
};
typedef class MapReduce TMapReduce, *PtrMapReduce;

#endif /* MAPREDUCE_H_ */
