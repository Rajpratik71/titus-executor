package state

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

func listPodToKeys(ctx context.Context, tx *bolt.Tx) error {
	fmt.Println("Listing pod -> key")
	pod2KeyBucket := tx.Bucket([]byte("pod2key"))
	if pod2KeyBucket == nil {
		return nil
	}
	pod2KeyBucket.ForEach(func(k, v []byte) error {
		_, err := fmt.Printf("%s -> %s\n", string(k), string(v))
		return err
	})

	return nil
}

func listKeyToPods(ctx context.Context, tx *bolt.Tx) error {
	fmt.Println("Listing key -> pod")
	pod2KeyBucket := tx.Bucket([]byte("key2pod"))
	if pod2KeyBucket == nil {
		return nil
	}
	pod2KeyBucket.ForEach(func(k, v []byte) error {
		_, err := fmt.Printf("%s -> %s\n", string(k), string(v))
		return err
	})

	return nil
}

func list(ctx context.Context, tx *bolt.Tx) error {
	err := listPodToKeys(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "Cannot list pod -> key mapping")
	}
	err = listKeyToPods(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "Cannot list key -> pod mapping")
	}

	return nil
}

func List(ctx context.Context, stateFile string) (retErr error) {
	db, err := bolt.Open(stateFile, 0644, &bolt.Options{Timeout: 10 * time.Second, ReadOnly: true})
	if err != nil {
		err = errors.Wrap(err, "Cannot open bolt DB")
		return err
	}
	defer func() {
		if retErr == nil {
			retErr = db.Close()
			if retErr != nil {
				retErr = errors.Wrap(err, "Cannot close bolt DB")
			}
		}
	}()

	err = db.View(func(tx *bolt.Tx) error {
		return list(ctx, tx)
	})

	return err
}
