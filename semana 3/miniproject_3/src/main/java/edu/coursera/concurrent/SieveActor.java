package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;

/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 *
 * countPrimes to determin the number of primes <= limit.
 */
public final class SieveActor extends Sieve {
    /**
     * {@inheritDoc}
     *
     * TODO Use the SieveActorActor class to calculate the number of primes <=
     * limit in parallel. You might consider how you can model the Sieve of
     * Eratosthenes as a pipeline of actors, each corresponding to a single
     * prime number.
     */
    @Override
    public int countPrimes(final int limit) {
        final SieveActorActor actor = new SieveActorActor();

        finish(() -> {
            for (int i = 3; i <= limit; i += 2) {
                actor.send(i);
            }
            actor.send(0);
        });

        // Sum up the number of local primes from each actor in the chain.
        int totalPrimes = 1;
        SieveActorActor currentActor = actor;
        while (currentActor != null) {
            totalPrimes += currentActor.getNumPrimes();
            currentActor = currentActor.nextActor;
        }

        return totalPrimes;
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor extends Actor {

        private static final int MAX_LOCAL_PRIMES = 1000;

        private int numPrimes;
        private int[] localPrimes;
        private SieveActorActor nextActor;

        public SieveActorActor() {
            this.numPrimes = 0;
            this.localPrimes = new int[MAX_LOCAL_PRIMES];
        }

        public int getNumPrimes() {
            return numPrimes;
        }

        /**
         * Process a single message sent to this actor.
         *
         * @param msg Received message
         */
        @Override
        public void process(final Object msg) {
            Integer candidate = (Integer) msg;
            if (candidate < 0 && nextActor != null) {
                nextActor.process(candidate);
            } else if (candidate < 0) {
                return;
            }

            final boolean isLocallyPrime = isLocallyPrime(candidate);

            if (isLocallyPrime) {
                if (numPrimes < MAX_LOCAL_PRIMES) {
                    localPrimes[numPrimes++] = candidate;
                } else if (nextActor != null) {
                    nextActor.send(candidate);
                } else {
                    nextActor = new SieveActorActor();
                    nextActor.send(candidate);
                }
            }
        }

        private boolean isLocallyPrime(Integer candidate) {
            for (int i = 0; i < numPrimes; i++) {
                if (candidate % localPrimes[i] == 0) {
                    return false;
                }
            }

            return true;
        }
    }
}