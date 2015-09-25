/** @file datastore.c */
#include "datastore.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

pthread_mutex_t dslock = PTHREAD_MUTEX_INITIALIZER;
unsigned long hash(const char *str) {
        unsigned long hash = 5381;
        int c;
        while (c = *str++) {
            hash = ((hash << 33) + hash) + c; /* hash * 33 + c */
	}
	if(hash >= 31333) {
		hash = (hash % 31333);
	}
        return hash;
}

int add(const char * key, const char * value, unsigned long revnum) {
	pthread_mutex_lock(&dslock);
	int idx = hash(key);
	if(!arr[idx]) {
		arr[idx] = malloc(sizeof(hash_entry));
		arr[idx]->next = NULL;
		arr[idx]->key = key;
		arr[idx]->value = value;
		arr[idx]->revisionnum = revnum;
		pthread_mutex_unlock(&dslock);
		return 0;

	}                                                            
	if(strcmp(arr[idx]->key, key) == 0) {
		pthread_mutex_unlock(&dslock);
		return 1;
	}
	else {
		int i;
		hash_entry * temp = arr[idx];
		for(i = 0; temp != NULL; i++) {
			if(strcmp(temp->key, key) == 0) {
				pthread_mutex_unlock(&dslock);
				return 1;
			}
			temp = temp->next;
		}
		temp = malloc(sizeof(hash_entry)); 
		temp->key = key;
		temp->value = value;
		temp->revisionnum = revnum;
		temp->next = arr[idx];
		arr[idx] = temp;
		pthread_mutex_unlock(&dslock);
		return 0;
	}
}
int update(const char * key, const char * value, unsigned long revnum) {
	pthread_mutex_lock(&dslock);
	int idx = hash(key); 
	int i;  
	hash_entry * temp = arr[idx];                                                         
	for(i = 0; temp != NULL; i++) {
		if(strcmp(temp->key, key) == 0) {
			if(temp->revisionnum == revnum) {
				(temp->revisionnum)++;
				 temp->value = value;
				 pthread_mutex_unlock(&dslock);
				 return temp->revisionnum;
			}
			else {
				pthread_mutex_unlock(&dslock);	
				return 0;			
			}
		}
		temp = temp->next;	
	}
	pthread_mutex_unlock(&dslock);
	return -1;
}

hash_entry * get(const char * key) { 
	pthread_mutex_lock(&dslock);	
	int idx = hash(key);
	int i;   
	hash_entry * temp = arr[idx];                                                       
	for(i = 0; temp != NULL; i++) {
		//printf("temp->key = %s\n", key);
		if(strcmp(temp->key, key) == 0) {
			pthread_mutex_unlock(&dslock);
			return temp;
		}
		temp = temp->next;
	}
	pthread_mutex_unlock(&dslock);
	return NULL; 
} 

int del (const char * key, const char * value) { 
	pthread_mutex_lock(&dslock);
	
	int index = hash(key);
	hash_entry * temp = arr[index];
	hash_entry * recurse_temp = temp;
	hash_entry * recurse_temp_next = NULL;

	if(!temp)
	{
		pthread_mutex_unlock(&dslock);
		return 0;
	}
	if(strcmp(temp -> key, key)==0)
	{
		arr[index] = temp->next;
		free(temp->key);
		free(temp->value);
		free(temp);
		pthread_mutex_unlock(&dslock);
		return 1;
	}
	int i;
	for(i = 0; recurse_temp != NULL; i++) {
		if(strcmp(recurse_temp-> key, key) == 0) {
			recurse_temp_next->next = recurse_temp->next;
			free(recurse_temp->key);
			free(recurse_temp->value);
			free(recurse_temp);
			pthread_mutex_unlock(&dslock);
			return 1;
		}
		else {
			recurse_temp_next = recurse_temp;
			recurse_temp = recurse_temp-> next;
		}	
	}
	pthread_mutex_unlock(&dslock);
	return 0;
}
# CS241-KVstore
