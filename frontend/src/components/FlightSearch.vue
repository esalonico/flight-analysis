<template>
  <div class="flight-search">
    <h2>Flight Search</h2>
    <form @submit.prevent="searchFlights">
      <div class="date-picker">
        <label for="flightDates">Flight Dates:</label>

        <VueDatePicker v-model="flightDates" model-auto id="flightDates" :partial-range="true"
          :range="{ noDisabledRange: true }" :disabled-dates="disabledDates" />

        <p v-if="flightDates">Selected date: {{ flightDates }}</p>
      </div>

      <div class="checkbox">
        <input type="checkbox" v-model="directFlights" id="directFlights" />
        <label for="directFlights">Direct Flights Only</label>
      </div>

      <button type="submit">Search Flights</button>
    </form>
  </div>
</template>

<script>
import VueDatePicker from '@vuepic/vue-datepicker';
import '@vuepic/vue-datepicker/dist/main.css';
import { ref } from 'vue';


export default {
  components: {
    VueDatePicker,
  },
  setup() {
    const flightDates = ref(null);
    const directFlights = ref(false);

    const disabledDates = (date) => {
      // Disable dates in the past
      const today = new Date();
      today.setHours(0, 0, 0, 0);
      return date < today;
    };

    const searchFlights = () => {
      const searchParams = {
        flightDates: flightDates.value,
        directFlights: directFlights.value,
      };
      console.log('Searching flights with parameters:', searchParams);
      // Add your flight search logic here
    };

    return {
      flightDates,
      directFlights,
      searchFlights,
      disabledDates,
    };
  },
};
</script>

<style>
.flight-search {
  max-width: 400px;
  margin: 0 auto;
  padding: 20px;
  border: 1px solid #ccc;
  border-radius: 10px;
}

.date-picker {
  margin-bottom: 20px;
}

.date-picker label {
  display: block;
  margin-bottom: 5px;
}

.checkbox {
  margin-bottom: 20px;
}

.button {
  display: block;
  width: 100%;
}

.datepicker-input {
  width: 100%;
  padding: 8px;
  margin-bottom: 10px;
  border: 1px solid #ccc;
  border-radius: 5px;
}
</style>
