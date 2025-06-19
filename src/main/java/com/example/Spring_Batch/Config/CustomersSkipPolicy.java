package com.example.Spring_Batch.Config;

import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;

import java.io.FileNotFoundException;

public class CustomersSkipPolicy implements SkipPolicy {



    @Override
    public boolean shouldSkip(Throwable t, long skipCount) throws SkipLimitExceededException {

        if(t instanceof FileNotFoundException) {
            System.out.println("File not found exception occurred, skipping the record.");
            return false;
        } else if (t instanceof NumberFormatException ) {
            return true;

        }else{
            System.out.println("An unexpected error occurred: " + t.getMessage());
            return false;
        }


    }
}
